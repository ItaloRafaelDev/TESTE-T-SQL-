USE FilaDB;
GO

/* Limpando */
TRUNCATE TABLE dbo.FilaExecucaoLog;
TRUNCATE TABLE dbo.FilaExecucao;

DECLARE @id1 BIGINT, @id2 BIGINT, @id3 BIGINT;

-- 1) Adicionar 3 tarefas: OK, FALHA (max 2), FLAKY (max 3)
EXEC dbo.AdicionarTarefa @NomeTarefa='OK',    @Payload=N'{}', @MaxTentativas=3;
SET @id1 = SCOPE_IDENTITY();

EXEC dbo.AdicionarTarefa @NomeTarefa='FALHA', @Payload=N'{}', @MaxTentativas=2;
SET @id2 = SCOPE_IDENTITY();

EXEC dbo.AdicionarTarefa @NomeTarefa='FLAKY', @Payload=N'{}', @MaxTentativas=3;
SET @id3 = SCOPE_IDENTITY();

-- ASSERT: 3 pendentes
IF (SELECT COUNT(*) FROM dbo.FilaExecucao WHERE Status='Pendente') <> 3
  THROW 60001, 'TESTE FALHOU: deveria ter 3 tarefas Pendentes.', 1;

-- 2) Rodar worker 1 (vai concluir OK, falhar FALHA e reagendar, falhar FLAKY e reagendar)
EXEC dbo.ExecutarTarefas @Worker='W1', @BatchSize=10, @BackoffSecondsBase=0;

-- ASSERT: OK concluída
IF (SELECT Status FROM dbo.FilaExecucao WHERE ID=@id1) <> 'Concluida'
  THROW 60002, 'TESTE FALHOU: tarefa OK deveria estar Concluida.', 1;

-- ASSERT: FALHA deve ter voltado pra Pendente (tent 1 de 2) OU Falhou (se max=1)
IF (SELECT Status FROM dbo.FilaExecucao WHERE ID=@id2) NOT IN ('Pendente','Falhou')
  THROW 60003, 'TESTE FALHOU: tarefa FALHA status inválido após 1 execução.', 1;

-- ASSERT: FLAKY deve ter voltado pra Pendente (tent 1 de 3)
IF (SELECT Status FROM dbo.FilaExecucao WHERE ID=@id3) <> 'Pendente'
  THROW 60004, 'TESTE FALHOU: tarefa FLAKY deveria ter voltado para Pendente após falhar 1x.', 1;

-- 3) Rodar worker 2 (FALHA falha de novo -> agora deve ir pra Falhou; FLAKY agora passa)
EXEC dbo.ExecutarTarefas @Worker='W2', @BatchSize=10, @BackoffSecondsBase=0;

-- ASSERT: FALHA final
IF (SELECT Status FROM dbo.FilaExecucao WHERE ID=@id2) <> 'Falhou'
  THROW 60005, 'TESTE FALHOU: tarefa FALHA deveria estar Falhou após estourar tentativas.', 1;

-- ASSERT: FLAKY concluída
IF (SELECT Status FROM dbo.FilaExecucao WHERE ID=@id3) <> 'Concluida'
  THROW 60006, 'TESTE FALHOU: tarefa FLAKY deveria estar Concluida na 2ª tentativa.', 1;

PRINT '✅ TODOS OS TESTES PASSARAM.';

-- Debug útil:
SELECT * FROM dbo.FilaExecucao ORDER BY ID;
SELECT * FROM dbo.FilaExecucaoLog ORDER BY LogID;
GO
