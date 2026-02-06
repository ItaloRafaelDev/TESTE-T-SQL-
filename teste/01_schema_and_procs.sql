/* =========================================================
   01 - Banco + Esquema da Fila + Procedures
   SQL Server 2022

   O que eu estou implementando aqui:
   - Uma fila transacional no SQL Server para enfileirar e executar tarefas.
   - Eu separo “claim” (reservar tarefas) de “execução” para lidar bem com concorrência.
   - Eu registro tudo em log para auditoria e para facilitar troubleshooting.

   Por que eu fiz desse jeito:
   - Se eu tentar executar e manter locks abertos por muito tempo, eu travo o banco.
   - Então eu faço uma transação curta só pra reservar tarefas (claim) e logo libero.
   - A execução acontece fora da transação longa, e eu só atualizo status ao final.

   Fluxo que eu sigo:
   Pendente -> Em andamento -> Concluida
                       \-> (falha) volta Pendente (reagendada) até estourar MaxTentativas -> Falhou
   ========================================================= */

USE master;
GO

/* Eu garanto que o banco exista antes de qualquer coisa.
   Eu executo isso em master para não depender do contexto da conexão. */
IF DB_ID('FilaDB') IS NULL
BEGIN
  EXEC('CREATE DATABASE FilaDB');
END
GO

USE FilaDB;
GO

/* =========================================================
   Tabela: FilaExecucao (a fila em si)

   Aqui eu guardo cada tarefa e o estado dela.
   Eu uso um ID identity como “ordem natural” (FIFO) para o claim.

   Observação importante:
   - Eu garanto a ordem na seleção/claim (ORDER BY ID).
   - Em execução concorrente, a conclusão pode ocorrer fora de ordem e isso é normal.
   ========================================================= */

IF OBJECT_ID('dbo.FilaExecucao','U') IS NOT NULL
  DROP TABLE dbo.FilaExecucao;
GO

CREATE TABLE dbo.FilaExecucao
(
  /* ID monotônico: me ajuda a manter FIFO no momento de escolher as tarefas (claim). */
  ID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_FilaExecucao PRIMARY KEY,

  /* Nome/Tipo da tarefa: eu uso isso para decidir qual lógica executar. */
  NomeTarefa        VARCHAR(100) NOT NULL,

  /* Status controlado por fluxo:
     - Pendente: pronta para ser pega por um worker
     - Em andamento: já foi reservada por um worker
     - Concluida: terminou com sucesso
     - Falhou: estourou tentativas e eu finalizei como falha */
  Status            VARCHAR(20)  NOT NULL,  -- 'Pendente', 'Em andamento', 'Concluida', 'Falhou'

  /* Payload livre (por exemplo JSON). Eu deixo NVARCHAR(MAX) para flexibilidade no teste. */
  Payload           NVARCHAR(MAX) NULL,

  /* Auditoria temporal: eu registro criação e última atualização da linha. */
  CriadoEm          DATETIME2(3) NOT NULL CONSTRAINT DF_Fila_CriadoEm DEFAULT SYSUTCDATETIME(),
  AtualizadoEm      DATETIME2(3) NOT NULL CONSTRAINT DF_Fila_AtualizadoEm DEFAULT SYSUTCDATETIME(),

  /* Eu marco quando a tarefa começou e terminou para analisar tempo de execução. */
  InicioEm          DATETIME2(3) NULL,
  FimEm             DATETIME2(3) NULL,

  /* Eu salvo o nome do worker que pegou/está executando (ajuda muito em debug). */
  Worker            SYSNAME NULL,

  /* Controle de retry:
     - Tentativas eu incremento no claim (antes de executar) para evitar race e dupla contagem
     - MaxTentativas define o limite de reexecução */
  Tentativas        INT NOT NULL CONSTRAINT DF_Fila_Tentativas DEFAULT (0),
  MaxTentativas     INT NOT NULL CONSTRAINT DF_Fila_MaxTentativas DEFAULT (3),

  /* Agendamento:
     - ProximoProcessamentoEm permite eu atrasar execução (backoff) em caso de falha
     - Também permite agendar tarefas futuras */
  ProximoProcessamentoEm DATETIME2(3) NOT NULL CONSTRAINT DF_Fila_ProximoProc DEFAULT SYSUTCDATETIME(),

  /* Último erro que eu capturei (curto) para diagnóstico. */
  UltimoErro        NVARCHAR(4000) NULL
);

/* Índice focado no padrão de leitura do claim:
   - Eu filtro por Status e ProximoProcessamentoEm
   - Eu ordeno por ID para manter FIFO no dequeuing */
CREATE INDEX IX_FilaExecucao_Status_Proximo_ID
ON dbo.FilaExecucao(Status, ProximoProcessamentoEm, ID);
GO

/* =========================================================
   Tabela: FilaExecucaoLog (auditoria)

   Aqui eu registro eventos por tarefa:
   - CLAIM: eu reservei a tarefa para um worker
   - START: eu comecei de fato a executar
   - SUCCESS: terminou OK
   - FAIL: falhou (reagendada ou final)

   Eu separo log da fila para manter histórico mesmo se eu limpar/reprocessar.
   ========================================================= */

IF OBJECT_ID('dbo.FilaExecucaoLog','U') IS NOT NULL
  DROP TABLE dbo.FilaExecucaoLog;
GO

CREATE TABLE dbo.FilaExecucaoLog
(
  LogID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_FilaExecucaoLog PRIMARY KEY,
  TarefaID BIGINT NOT NULL,
  Evento   VARCHAR(30) NOT NULL,
  Mensagem NVARCHAR(4000) NULL,
  Em       DATETIME2(3) NOT NULL CONSTRAINT DF_Log_Em DEFAULT SYSUTCDATETIME(),
  Worker   SYSNAME NULL
);

CREATE INDEX IX_Log_TarefaID ON dbo.FilaExecucaoLog(TarefaID);
GO

/* =========================================================
   Procedure: AdicionarTarefa

   Eu insiro uma nova tarefa como 'Pendente' e já defino quando ela pode rodar.
   Se não vier ProximoProcessamentoEm, eu assumo "agora" (execução imediata).
   ========================================================= */

IF OBJECT_ID('dbo.AdicionarTarefa','P') IS NOT NULL
  DROP PROC dbo.AdicionarTarefa;
GO

CREATE PROC dbo.AdicionarTarefa
  @NomeTarefa VARCHAR(100),
  @Payload NVARCHAR(MAX) = NULL,
  @MaxTentativas INT = 3,
  @ProximoProcessamentoEm DATETIME2(3) = NULL
AS
BEGIN
  SET NOCOUNT ON;

  /* Se não foi informado, eu deixo a tarefa elegível imediatamente. */
  IF @ProximoProcessamentoEm IS NULL
    SET @ProximoProcessamentoEm = SYSUTCDATETIME();

  INSERT INTO dbo.FilaExecucao
  (
    NomeTarefa,
    Status,
    Payload,
    MaxTentativas,
    ProximoProcessamentoEm
  )
  VALUES
  (
    @NomeTarefa,
    'Pendente',
    @Payload,
    @MaxTentativas,
    @ProximoProcessamentoEm
  );

  /* Eu retorno o ID gerado para quem chamou conseguir rastrear a tarefa. */
  SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS TarefaID;
END
GO

/* =========================================================
   Procedure: _RunTask (simulador)

   Aqui eu simulo a execução real para poder testar o fluxo.
   No mundo real, eu substituiria isso por lógica específica por tipo de tarefa.

   Eu deixo determinístico para testes automatizados:
   - OK: sempre sucesso
   - FALHA: sempre falha
   - FLAKY: falha na 1ª tentativa e depois passa
   ========================================================= */

IF OBJECT_ID('dbo._RunTask','P') IS NOT NULL
  DROP PROC dbo._RunTask;
GO

CREATE PROC dbo._RunTask
  @TarefaID BIGINT,
  @NomeTarefa VARCHAR(100),
  @Payload NVARCHAR(MAX)
AS
BEGIN
  SET NOCOUNT ON;

  IF @NomeTarefa = 'OK'
    RETURN;

  IF @NomeTarefa = 'FALHA'
    THROW 51001, 'Tarefa configurada para falhar.', 1;

  IF @NomeTarefa = 'FLAKY'
  BEGIN
    /* Como eu incremento Tentativas no claim, Tentativas=1 significa primeira execução. */
    DECLARE @tent INT = (SELECT Tentativas FROM dbo.FilaExecucao WHERE ID = @TarefaID);

    IF @tent <= 1
      THROW 51002, 'Falha transitória simulada.', 1;

    RETURN;
  END

  /* Qualquer outro NomeTarefa eu trato como sucesso neste simulador. */
  RETURN;
END
GO

/* =========================================================
   Procedure: ExecutarTarefas

   O que eu garanto aqui:
   - Eu consigo rodar vários workers em paralelo sem executar a mesma tarefa duas vezes.
   - Eu faço o claim com locks apropriados (UPDLOCK/READPAST) em uma transação curta.
   - Eu executo fora da transação para não segurar lock por muito tempo.
   - Eu trato falhas com retry e backoff simples.

   Por que eu uso READPAST + UPDLOCK:
   - UPDLOCK impede que outro worker pegue a mesma linha enquanto eu estou "reservando".
   - READPAST faz eu pular linhas que estejam bloqueadas por outro worker (melhora throughput).
   - ROWLOCK é uma sugestão para favorecer lock por linha (não é garantia absoluta).
   ========================================================= */

IF OBJECT_ID('dbo.ExecutarTarefas','P') IS NOT NULL
  DROP PROC dbo.ExecutarTarefas;
GO

CREATE PROC dbo.ExecutarTarefas
  @Worker SYSNAME,
  @BatchSize INT = 5,
  @BackoffSecondsBase INT = 10  -- eu aplico backoff linear: base * Tentativas
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON; -- se der erro dentro da transação, eu não quero estado parcial

  DECLARE @now DATETIME2(3) = SYSUTCDATETIME();

  /* Eu guardo o lote claimado aqui para executar sem depender de reconsulta. */
  DECLARE @claimed TABLE
  (
    ID BIGINT PRIMARY KEY,
    NomeTarefa VARCHAR(100),
    Payload NVARCHAR(MAX),
    Tentativas INT,
    MaxTentativas INT
  );

  /* =========================================================
     1) CLAIM ATÔMICO (curto)
     Eu seleciono TOP(@BatchSize) tarefas Pendentes elegíveis e marco como Em andamento.
     Eu faço isso em transação curta para evitar que dois workers peguem a mesma tarefa.
     ========================================================= */
  BEGIN TRAN;

    ;WITH cte AS
    (
      SELECT TOP (@BatchSize) *
      FROM dbo.FilaExecucao WITH (READPAST, UPDLOCK, ROWLOCK)
      WHERE Status = 'Pendente'
        AND ProximoProcessamentoEm <= @now
      ORDER BY ID ASC
    )
    UPDATE cte
      SET Status = 'Em andamento',
          AtualizadoEm = @now,
          InicioEm = COALESCE(InicioEm, @now),
          Worker = @Worker,
          Tentativas = Tentativas + 1
    OUTPUT
      inserted.ID,
      inserted.NomeTarefa,
      inserted.Payload,
      inserted.Tentativas,
      inserted.MaxTentativas
    INTO @claimed(ID, NomeTarefa, Payload, Tentativas, MaxTentativas);

    /* Eu registro o claim para auditoria e para provar que não houve duplicidade. */
    INSERT INTO dbo.FilaExecucaoLog(TarefaID, Evento, Mensagem, Worker)
    SELECT
      ID,
      'CLAIM',
      CONCAT('Claimed para execução. Tentativa=', Tentativas),
      @Worker
    FROM @claimed;

  COMMIT;

  /* =========================================================
     2) EXECUÇÃO (fora de transação longa)
     Eu executo tarefa por tarefa do lote claimado.
     Em sucesso: Concluida
     Em falha: ou eu reagendo (volta para Pendente) ou eu finalizo como Falhou.
     ========================================================= */
  DECLARE
    @id BIGINT,
    @nome VARCHAR(100),
    @payload NVARCHAR(MAX),
    @tent INT,
    @maxTent INT;

  /* Eu uso cursor porque o batch é pequeno e eu quero controle por tarefa.
     Isso deixa o fluxo muito legível para o teste técnico. */
  DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT ID, NomeTarefa, Payload, Tentativas, MaxTentativas
    FROM @claimed
    ORDER BY ID; -- eu mantenho a ordem FIFO do lote claimado

  OPEN cur;
  FETCH NEXT FROM cur INTO @id, @nome, @payload, @tent, @maxTent;

  WHILE @@FETCH_STATUS = 0
  BEGIN
    BEGIN TRY
      INSERT INTO dbo.FilaExecucaoLog(TarefaID, Evento, Mensagem, Worker)
      VALUES (@id, 'START', 'Iniciando execução.', @Worker);

      EXEC dbo._RunTask @TarefaID = @id, @NomeTarefa = @nome, @Payload = @payload;

      UPDATE dbo.FilaExecucao
        SET Status = 'Concluida',
            AtualizadoEm = SYSUTCDATETIME(),
            FimEm = SYSUTCDATETIME(),
            UltimoErro = NULL
      WHERE ID = @id;

      INSERT INTO dbo.FilaExecucaoLog(TarefaID, Evento, Mensagem, Worker)
      VALUES (@id, 'SUCCESS', 'Execução concluída com sucesso.', @Worker);
    END TRY
    BEGIN CATCH
      DECLARE @err NVARCHAR(4000) = ERROR_MESSAGE();

      /* Eu calculo o próximo horário usando backoff linear.
         Em produção eu poderia usar backoff exponencial com jitter, mas aqui eu mantenho simples. */
      DECLARE @next DATETIME2(3) =
        DATEADD(SECOND, @BackoffSecondsBase * @tent, SYSUTCDATETIME());

      IF @tent < @maxTent
      BEGIN
        /* Ainda tenho tentativas: eu volto para Pendente e reagendo para o futuro. */
        UPDATE dbo.FilaExecucao
          SET Status = 'Pendente',
              AtualizadoEm = SYSUTCDATETIME(),
              ProximoProcessamentoEm = @next,
              UltimoErro = @err
        WHERE ID = @id;

        INSERT INTO dbo.FilaExecucaoLog(TarefaID, Evento, Mensagem, Worker)
        VALUES
        (
          @id,
          'FAIL',
          CONCAT('Falha: ', @err, ' | Reagendado para ', CONVERT(VARCHAR(33), @next, 126)),
          @Worker
        );
      END
      ELSE
      BEGIN
        /* Estourou tentativas: eu marco como Falhou e finalizo. */
        UPDATE dbo.FilaExecucao
          SET Status = 'Falhou',
              AtualizadoEm = SYSUTCDATETIME(),
              FimEm = SYSUTCDATETIME(),
              UltimoErro = @err
        WHERE ID = @id;

        INSERT INTO dbo.FilaExecucaoLog(TarefaID, Evento, Mensagem, Worker)
        VALUES
        (
          @id,
          'FAIL',
          CONCAT('Falha final (sem tentativas restantes): ', @err),
          @Worker
        );
      END
    END CATCH;

    FETCH NEXT FROM cur INTO @id, @nome, @payload, @tent, @maxTent;
  END

  CLOSE cur;
  DEALLOCATE cur;

  /* Eu retorno o lote claimado porque isso ajuda a depurar e validar testes. */
  SELECT * FROM @claimed ORDER BY ID;
END
GO
/* =========================================================
   Fim do script de criação do banco, esquema e procedures.
   ========================================================= */
