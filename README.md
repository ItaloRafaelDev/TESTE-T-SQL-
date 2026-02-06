# Fila transacional no SQL Server

Eu implementei uma fila transacional no SQL Server 2022 com foco em concorrencia segura, rastreabilidade e simplicidade de teste. A ideia e separar o `claim` (reservar tarefas) da execucao para evitar locks longos e permitir varios workers em paralelo sem duplicar processamento.

**O que tem aqui**
- `teste/01_schema_and_procs.sql`: cria o banco `FilaDB`, as tabelas `FilaExecucao` e `FilaExecucaoLog`, e as procedures.
- `teste/02_tests.sql`: roteiro de testes com asserts para validar o fluxo de sucesso, falha e retry.
- `sql-fila/docker-compose.yml`: sobe um SQL Server 2022 (Developer) em container.

**Como subir o SQL Server**
1. `cd sql-fila`
1. `docker compose up -d`

O SQL Server fica em `localhost:1433` com usuario `sa` e senha `teste81933224!`.

**Como criar o banco e o esquema (comando direto)**
Eu rodo o script pelo `sqlcmd` dentro do container:
```powershell
Get-Content teste/01_schema_and_procs.sql | docker exec -i sqlserver-fila /opt/mssql-tools18/bin/sqlcmd `
  -S localhost -U sa -P "teste81933224!" -C
```

Esse script cria o banco `FilaDB` se nao existir e recria as tabelas e procedures. Ele apaga as tabelas `dbo.FilaExecucao` e `dbo.FilaExecucaoLog` se ja existirem.

**Como rodar os testes (comando direto)**
```powershell
Get-Content teste/02_tests.sql | docker exec -i sqlserver-fila /opt/mssql-tools18/bin/sqlcmd `
  -S localhost -U sa -P "teste81933224!" -C -d FilaDB
```

Os testes criam tres tarefas (`OK`, `FALHA`, `FLAKY`), rodam dois workers e validam:
- `OK` conclui na primeira execucao.
- `FALHA` falha ate estourar `MaxTentativas` e termina como `Falhou`.
- `FLAKY` falha na primeira tentativa e conclui na segunda.

**Como rodar dois workers em paralelo**
Terminal A:
```powershell
docker exec -it sqlserver-fila /opt/mssql-tools18/bin/sqlcmd `
  -S localhost -U sa -P "teste81933224!" -C -d FilaDB -Q "
  EXEC dbo.ExecutarTarefas @Worker='WA', @BatchSize=25, @BackoffSecondsBase=0;"
```

Terminal B:
```powershell
docker exec -it sqlserver-fila /opt/mssql-tools18/bin/sqlcmd `
  -S localhost -U sa -P "teste81933224!" -C -d FilaDB -Q "
  EXEC dbo.ExecutarTarefas @Worker='WB', @BatchSize=25, @BackoffSecondsBase=0;"
```

**Prova de que nao houve duplicidade (assert)**
Se voltar linhas no ultimo SELECT, houve duplicidade. O esperado e 0 linhas.
```powershell
docker exec -it sqlserver-fila /opt/mssql-tools18/bin/sqlcmd `
  -S localhost -U sa -P "teste81933224!" -C -d FilaDB -Q "
  SELECT COUNT(*) AS Total FROM dbo.FilaExecucao;
  SELECT COUNT(*) AS Concluidas FROM dbo.FilaExecucao WHERE Status='Concluida';
  SELECT TarefaID, COUNT(*) AS StartCount
  FROM dbo.FilaExecucaoLog
  WHERE Evento='START'
  GROUP BY TarefaID
  HAVING COUNT(*) > 1;"
```

**Como funciona o fluxo**
- Estados possiveis: `Pendente` -> `Em andamento` -> `Concluida` ou `Falhou`.
- O claim e feito com `READPAST` + `UPDLOCK` em transacao curta e ordenado por `ID` para manter FIFO no dequeuing.
- A execucao acontece fora da transacao longa; no final eu atualizo o status.
- Em caso de falha, aplico backoff linear: `ProximoProcessamentoEm = agora + (base * Tentativas)`.

**Simulador de execucao**
Eu usei a procedure `_RunTask` para simular o comportamento:
- `OK`: sempre sucesso.
- `FALHA`: sempre falha.
- `FLAKY`: falha na 1a tentativa e passa depois.
