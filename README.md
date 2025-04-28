# MongoXsql - Migração de MongoDB para PostgreSQL

Uma ferramenta robusta para migrar dados de MongoDB para PostgreSQL, desenvolvida em Go.

## Características

- Migração de collections MongoDB (arquivos BSON e JSON) para tabelas PostgreSQL
- Inferência automática de esquema baseada nos documentos
- Suporte para múltiplos formatos de exportação (BSON e JSON)
- Compatível com JSON Arrays e JSON Lines (um documento por linha)
- Opções para incluir ou excluir collections específicas
- Configuração flexível via flags de linha de comando
- Possibilidade de substituir tabelas existentes ou preservá-las

## Requisitos

- Go 1.16 ou superior
- PostgreSQL 10 ou superior
- Arquivos BSON/JSON exportados do MongoDB

## Instalação

1. Clone o repositório:
```bash
cd mongoXpostgres
```

2. Compile o programa (opcional):
```bash
go build -o mongoXsql
```

## Configuração

1. Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```
POSTGRES_USER=seu_usuario
POSTGRES_PASSWORD=sua_senha
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=seu_banco
POSTGRES_SSLMODE=disable
```

## Uso

### Sintaxe básica

```bash
go run main.go -path <caminho-para-arquivos>
```

Ou se você compilou o programa:

```bash
./mongoXsql -path <caminho-para-arquivos>
```

### Flags disponíveis

| Flag | Descrição | Exemplo |
|------|-----------|---------|
| `-path` | Caminho para o diretório com arquivos BSON/JSON (obrigatório) | `-path ./dados_mongodb` |
| `-drop` | Apaga tabelas existentes antes de criar novas | `-drop` |
| `-collections` | Lista de collections para migrar (separadas por vírgula) | `-collections usuarios,produtos` |
| `-skip` | Lista de collections para ignorar (separadas por vírgula) | `-skip logs,temp` |
| `-env` | Caminho para o arquivo .env (padrão: .env na raiz) | `-env ./config/prod.env` |

## Exemplos

### Migração básica
```bash
go run main.go -path ./dados_mongodb
```

### Substituir tabelas existentes
```bash
go run main.go -path ./dados_mongodb -drop
```

### Migrar apenas collections específicas
```bash
go run main.go -path ./dados_mongodb -collections "usuarios,produtos,pedidos"
```

### Ignorar collections específicas
```bash
go run main.go -path ./dados_mongodb -skip "logs,sessoes,temp"
```

### Usar arquivo .env personalizado
```bash
go run main.go -path ./dados_mongodb -env ./config/prod.env
```

## Como funciona

1. O programa lê todos os arquivos BSON e/ou JSON do diretório especificado
2. Para cada arquivo (collection), infere o esquema da tabela PostgreSQL
3. Cria as tabelas correspondentes no PostgreSQL
4. Migra os dados, convertendo os tipos do MongoDB para tipos PostgreSQL
5. Cada tabela receberá:
   - Uma coluna `id` primária UUID
   - Uma coluna `mongo_id` para o ID original do MongoDB
   - Colunas `created_at` e `updated_at` com timestamps

## Considerações

- Campos com tipos mistos são convertidos para JSONB no PostgreSQL
- O valor de `_id` do MongoDB é migrado para a coluna `mongo_id` (como TEXT)
- As colunas inferidas são sempre NULLABLE para maior compatibilidade
- Se houver erros durante a inserção de dados, o programa continuará com os próximos documentos

## Limitações

- O programa não migra índices ou constraints além da chave primária
- Relacionamentos entre collections não são preservados automaticamente
- A ferramenta foi testada principalmente com dados exportados via mongodump

## Solução de problemas

Se encontrar algum erro:

1. Verifique se as credenciais do PostgreSQL estão corretas no arquivo `.env`
2. Certifique-se de que o PostgreSQL está acessível e aceitando conexões
3. Verifique se o caminho dos arquivos exportados está correto
4. Verifique se os arquivos BSON/JSON são válidos e foram exportados corretamente
5. Se uma tabela falhar na migração, tente usar a flag `-drop` para recriá-la 