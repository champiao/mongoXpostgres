package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	// Adicionaremos mais imports depois: biblioteca bson
)

// Configurações
type Config struct {
	DataPath        string
	DropExisting    bool
	OnlyCollections []string
	SkipCollections []string
}

func main() {
	// Configuração via flags
	config := Config{}
	flag.StringVar(&config.DataPath, "path", "", "Caminho para os arquivos BSON/JSON exportados do MongoDB")
	flag.BoolVar(&config.DropExisting, "drop", false, "Apagar tabelas existentes antes de criar novas")
	collectionsFlag := flag.String("collections", "", "Lista de collections para migrar, separadas por vírgula (opcional)")
	skipFlag := flag.String("skip", "", "Lista de collections para pular, separadas por vírgula (opcional)")
	envFile := flag.String("env", ".env", "Caminho para o arquivo .env")
	flag.Parse()

	if config.DataPath == "" {
		log.Fatal("Erro: O caminho para os arquivos exportados do MongoDB é obrigatório. Use a flag -path")
	}

	// Processar listas de collections
	if *collectionsFlag != "" {
		config.OnlyCollections = strings.Split(*collectionsFlag, ",")
		for i, col := range config.OnlyCollections {
			config.OnlyCollections[i] = strings.TrimSpace(col)
		}
	}
	if *skipFlag != "" {
		config.SkipCollections = strings.Split(*skipFlag, ",")
		for i, col := range config.SkipCollections {
			config.SkipCollections[i] = strings.TrimSpace(col)
		}
	}

	// Carregar variáveis de ambiente do .env
	err := godotenv.Load(*envFile)
	if err != nil {
		log.Printf("Aviso: Não foi possível encontrar o arquivo .env em %s\n", *envFile)
	}

	// Ler variáveis de conexão do ambiente
	user := os.Getenv("POSTGRES_USER")
	pwd := os.Getenv("POSTGRES_PASSWORD")
	host := os.Getenv("POSTGRES_HOST")
	port := os.Getenv("POSTGRES_PORT")
	dbname := os.Getenv("POSTGRES_DB")
	sslmode := os.Getenv("POSTGRES_SSLMODE")
	if sslmode == "" {
		sslmode = "disable"
	}

	// Validar se as variáveis essenciais foram definidas
	requiredVars := map[string]string{
		"POSTGRES_USER": user,
		"POSTGRES_HOST": host,
		"POSTGRES_PORT": port,
		"POSTGRES_DB":   dbname,
	}
	var missingVars []string
	for key, value := range requiredVars {
		if value == "" {
			missingVars = append(missingVars, key)
		}
	}
	if len(missingVars) > 0 {
		log.Fatalf("Erro: As seguintes variáveis de ambiente PostgreSQL estão faltando: %s", strings.Join(missingVars, ", "))
	}

	// Construir a string de conexão (DSN)
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		user, pwd, host, port, dbname, sslmode)

	fmt.Println("Conectando ao PostgreSQL...")

	// 1. Conectar ao PostgreSQL
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("Não foi possível conectar ao PostgreSQL: %v\n", err)
	}
	defer conn.Close(ctx)

	fmt.Println("Conectado com sucesso!")

	// --- Garantir extensões necessárias ---
	fmt.Println("Garantindo extensão uuid-ossp...")
	_, err = conn.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`)
	if err != nil {
		log.Fatalf("Erro ao criar extensão uuid-ossp: %v\n", err)
	}

	// --- Processamento das Collections ---
	fmt.Printf("\nIniciando processamento das collections em: %s\n", config.DataPath)

	// Verificar se o diretório existe
	_, err = os.Stat(config.DataPath)
	if os.IsNotExist(err) {
		log.Fatalf("Erro: O diretório %s não existe", config.DataPath)
	}

	// Listar arquivos no diretório
	files, err := os.ReadDir(config.DataPath)
	if err != nil {
		log.Fatalf("Erro ao listar diretório %s: %v", config.DataPath, err)
	}

	// Verificar se existem arquivos BSON ou JSON
	hasBSON := false
	hasJSON := false
	for _, file := range files {
		if !file.IsDir() {
			if strings.HasSuffix(file.Name(), ".bson") {
				hasBSON = true
			} else if strings.HasSuffix(file.Name(), ".json") {
				hasJSON = true
			}
		}
	}

	if !hasBSON && !hasJSON {
		log.Fatalf("Erro: Nenhum arquivo BSON ou JSON encontrado em %s", config.DataPath)
	}

	// Processar cada arquivo
	for _, file := range files {
		if file.IsDir() {
			continue // Pula diretórios
		}

		isBSON := strings.HasSuffix(file.Name(), ".bson")
		isJSON := strings.HasSuffix(file.Name(), ".json")

		if !isBSON && !isJSON {
			continue // Pula arquivos que não são BSON nem JSON
		}

		fileName := file.Name()
		// Remove a extensão para obter o nome da collection
		collectionName := strings.TrimSuffix(fileName, filepath.Ext(fileName))
		filePath := filepath.Join(config.DataPath, fileName)
		sanitizedTableName := strings.ToLower(collectionName)

		// Verificar se deve pular esta collection
		if shouldSkip(collectionName, config.OnlyCollections, config.SkipCollections) {
			fmt.Printf("Pulando collection: %s (filtrada por configuração)\n", collectionName)
			continue
		}

		fmt.Printf("\n--- Processando Collection: %s (Arquivo: %s) ---\n", collectionName, fileName)

		// Verificar se a tabela já existe
		var tableExists bool
		err := conn.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_schema = 'public' 
				AND table_name = $1
			)
		`, sanitizedTableName).Scan(&tableExists)

		if err != nil {
			log.Printf("ERRO ao verificar se a tabela %s existe: %v", sanitizedTableName, err)
			continue
		}

		if tableExists {
			if config.DropExisting {
				fmt.Printf("Tabela '%s' já existe. Apagando conforme configurado...\n", sanitizedTableName)
				_, err = conn.Exec(ctx, fmt.Sprintf(`DROP TABLE public."%s" CASCADE`, sanitizedTableName))
				if err != nil {
					log.Printf("ERRO ao apagar tabela %s: %v. Pulando esta collection.", sanitizedTableName, err)
					continue
				}
			} else {
				fmt.Printf("Tabela '%s' já existe. Use a flag -drop para recriar. Pulando...\n", sanitizedTableName)
				continue
			}
		}

		// Ler arquivo BSON ou JSON
		var docs []bson.M
		if isBSON {
			fmt.Println("Lendo arquivo BSON...")
			docs, err = readBSONFile(filePath)
		} else { // isJSON
			fmt.Println("Lendo arquivo JSON...")
			docs, err = readJSONFile(filePath)
		}

		if err != nil {
			log.Printf("ERRO ao ler %s: %v. Pulando esta collection.", fileName, err)
			continue
		}
		fmt.Printf("Lidos %d documentos.\n", len(docs))

		if len(docs) == 0 {
			log.Printf("Aviso: Nenhum documento encontrado em %s. Pulando criação de tabela e migração.", fileName)
			continue
		}

		// Inferir schema
		fmt.Println("Inferindo schema...")
		inferredSchema := inferSchema(docs)

		// Gerar SQL CREATE TABLE
		fmt.Println("Gerando SQL CREATE TABLE...")
		createTableSQL := generateCreateTableSQL(sanitizedTableName, inferredSchema)

		// Executar SQL gerado no PostgreSQL
		fmt.Println("Executando SQL CREATE TABLE no PostgreSQL...")
		_, err = conn.Exec(ctx, createTableSQL)
		if err != nil {
			log.Printf("ERRO ao executar CREATE TABLE para %s: %v. Pulando migração de dados.", sanitizedTableName, err)
			continue
		}
		fmt.Println("Tabela criada com sucesso!")

		// Migrar dados para PostgreSQL
		fmt.Printf("Iniciando migração de %d documentos para a tabela '%s'...\n", len(docs), sanitizedTableName)
		err = migrateData(ctx, conn, sanitizedTableName, inferredSchema, docs)
		if err != nil {
			log.Printf("ERRO durante a migração de dados para %s: %v", sanitizedTableName, err)
		} else {
			fmt.Printf("Migração de dados para '%s' concluída com sucesso!\n", sanitizedTableName)
		}
	}

	fmt.Println("\nProcessamento de todas as collections concluído.")
}

// shouldSkip verifica se uma collection deve ser pulada com base nas listas de inclusão e exclusão
func shouldSkip(collectionName string, onlyCollections, skipCollections []string) bool {
	// Se há uma lista de collections específicas e esta não está nela
	if len(onlyCollections) > 0 {
		found := false
		for _, allowed := range onlyCollections {
			if strings.EqualFold(collectionName, allowed) {
				found = true
				break
			}
		}
		if !found {
			return true
		}
	}

	// Se está na lista de collections a serem puladas
	for _, skip := range skipCollections {
		if strings.EqualFold(collectionName, skip) {
			return true
		}
	}

	return false
}

// readJSONFile lê um arquivo JSON contendo uma array de documentos
func readJSONFile(filePath string) ([]bson.M, error) {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("falha ao abrir arquivo %s: %w", filePath, err)
	}

	// Verificar se o JSON contém um array ou um documento por linha
	trimmedContent := strings.TrimSpace(string(content))
	if strings.HasPrefix(trimmedContent, "[") && strings.HasSuffix(trimmedContent, "]") {
		// É um array JSON
		var docs []bson.M
		if err := json.Unmarshal(content, &docs); err != nil {
			return nil, fmt.Errorf("falha ao decodificar array JSON em %s: %w", filePath, err)
		}
		return docs, nil
	} else {
		// Formato de linha por linha (JSON Lines)
		var docs []bson.M
		scanner := bufio.NewScanner(strings.NewReader(string(content)))
		lineNum := 0
		for scanner.Scan() {
			lineNum++
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "//") {
				continue // Pular linhas vazias ou comentários
			}

			var doc bson.M
			if err := json.Unmarshal([]byte(line), &doc); err != nil {
				log.Printf("Aviso: Erro ao decodificar JSON na linha %d de %s: %v. Pulando esta linha.", lineNum, filePath, err)
				continue
			}
			docs = append(docs, doc)
		}

		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("erro ao ler linhas do arquivo %s: %w", filePath, err)
		}

		return docs, nil
	}
}

// readBSONFile lê um arquivo contendo múltiplos documentos BSON concatenados.
func readBSONFile(filePath string) ([]bson.M, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("falha ao abrir arquivo %s: %w", filePath, err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var docs []bson.M

	for {
		// Ler o tamanho do documento (primeiros 4 bytes, little-endian)
		var sizeBytes [4]byte
		n, err := io.ReadFull(reader, sizeBytes[:])
		if err == io.EOF {
			break // Fim do arquivo
		}
		if err != nil || n < 4 {
			if err == io.ErrUnexpectedEOF || (err == nil && n < 4) {
				// Pode ser um final de arquivo truncado ou um erro genuíno
				log.Printf("Aviso: Leitura incompleta do tamanho do documento BSON em %s, pode ter chegado ao fim inesperadamente (%d bytes lidos).", filePath, n)
				break
			}
			return nil, fmt.Errorf("falha ao ler tamanho do documento BSON em %s: %w", filePath, err)
		}

		docSize := binary.LittleEndian.Uint32(sizeBytes[:])
		if docSize < 5 { // Tamanho mínimo de um documento BSON ({}) é 5 bytes
			return nil, fmt.Errorf("tamanho inválido de documento BSON (%d bytes) lido em %s", docSize, filePath)
		}

		// Alocar buffer para o documento completo (incluindo os 4 bytes de tamanho)
		docData := make([]byte, docSize)
		copy(docData, sizeBytes[:]) // Copia os bytes de tamanho já lidos

		// Ler o restante do documento
		if _, err := io.ReadFull(reader, docData[4:]); err != nil {
			return nil, fmt.Errorf("falha ao ler corpo do documento BSON (tamanho %d) em %s: %w", docSize, filePath, err)
		}

		// Decodificar o documento BSON
		var doc bson.M
		if err := bson.Unmarshal(docData, &doc); err != nil {
			// Tentar decodificar como Raw se Unmarshal falhar (pode ajudar a depurar)
			var rawDoc bson.Raw
			if rawErr := bson.Unmarshal(docData, &rawDoc); rawErr == nil {
				log.Printf("Erro ao decodificar documento BSON em %s, mas bson.Raw funcionou: %v\nDocumento Raw: %s", filePath, err, rawDoc.String())
			} else {
				log.Printf("Erro ao decodificar documento BSON em %s: %v\nBytes (hex): %x", filePath, err, docData)
			}
			return nil, fmt.Errorf("falha ao decodificar documento BSON em %s: %w", filePath, err)
		}
		docs = append(docs, doc)
	}

	return docs, nil
}

// inferSchema analisa documentos BSON e infere um schema SQL.
func inferSchema(docs []bson.M) map[string]string {
	if len(docs) == 0 {
		return make(map[string]string)
	}

	// fieldTypes armazena a contagem de cada tipo Go visto para cada nome de campo
	// Ex: fieldTypes["age"][reflect.TypeOf(int32(0))] = 10
	fieldTypes := make(map[string]map[reflect.Type]int)

	for _, doc := range docs {
		for key, value := range doc {
			if fieldTypes[key] == nil {
				fieldTypes[key] = make(map[reflect.Type]int)
			}
			// Usar reflect.TypeOf(nil) se o valor for nil explicitamente
			var valueType reflect.Type
			if value == nil {
				// Marcador especial para nil, não podemos chamar TypeOf(nil)
				valueType = reflect.TypeOf((*interface{})(nil)).Elem() // Um tipo distinto para nil
			} else {
				valueType = reflect.TypeOf(value)
			}
			fieldTypes[key][valueType]++
		}
	}

	inferredSQLSchema := make(map[string]string)
	for fieldName, typeCounts := range fieldTypes {
		if fieldName == "_id" {
			inferredSQLSchema["mongo_id"] = "TEXT" // Mapeia _id para mongo_id como TEXT
			continue                               // Pula o processamento adicional para _id
		}

		var dominantType reflect.Type
		maxCount := 0
		hasNil := false
		multipleTypes := len(typeCounts) > 1

		for typ, count := range typeCounts {
			if typ == reflect.TypeOf((*interface{})(nil)).Elem() {
				hasNil = true
				// Não consideramos nil como dominante, mas anotamos sua presença
				continue
			}
			if count > maxCount {
				maxCount = count
				dominantType = typ
			}
		}

		// Se só vimos nil, ou não vimos nenhum tipo (campo sempre nil?)
		if dominantType == nil {
			// Vamos assumir TEXT como um palpite seguro se só vimos nulos.
			// Ou poderíamos omitir a coluna, mas TEXT é mais flexível.
			inferredSQLSchema[fieldName] = "TEXT" // Ou talvez JSONB? Ou omitir?
			log.Printf("Aviso: Campo '%s' parece ser sempre nulo ou ausente. Mapeando para TEXT.", fieldName)
			continue
		}

		// Se vários tipos foram vistos (excluindo nil), usar JSONB
		if multipleTypes && !(multipleTypes && len(typeCounts) == 2 && hasNil) { // Permite um tipo + nil
			// Exceção: Se for int32 e int64, podemos promover para BIGINT.
			_, hasInt32 := typeCounts[reflect.TypeOf(int32(0))]
			_, hasInt64 := typeCounts[reflect.TypeOf(int64(0))]
			if hasInt32 && hasInt64 && len(typeCounts) <= 3 { // permite int32, int64, e talvez nil
				inferredSQLSchema[fieldName] = "BIGINT"
				log.Printf("Info: Campo '%s' tem tipos int32 e int64. Promovendo para BIGINT.", fieldName)
			} else {
				inferredSQLSchema[fieldName] = "JSONB"
				log.Printf("Aviso: Campo '%s' tem múltiplos tipos (%v). Mapeando para JSONB.", fieldName, typeCounts)
			}
		} else {
			// Mapear o tipo dominante para SQL
			inferredSQLSchema[fieldName] = mapGoTypeToSQL(dominantType)
		}
	}

	return inferredSQLSchema
}

// mapGoTypeToSQL converte um tipo Go (refletido) para um tipo string SQL PostgreSQL.
func mapGoTypeToSQL(t reflect.Type) string {
	switch t.Kind() {
	case reflect.String:
		return "TEXT"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return "INTEGER"
	case reflect.Int64:
		return "BIGINT"
	case reflect.Float32, reflect.Float64:
		// Considerar NUMERIC/DECIMAL se precisão for crítica?
		return "DOUBLE PRECISION"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Slice, reflect.Array:
		// Arrays/Slices BSON mapeiam bem para JSONB
		// Poderia verificar o tipo do elemento, mas JSONB é mais geral.
		if t.Elem().Kind() == reflect.Uint8 { // []byte
			return "BYTEA"
		}
		return "JSONB"
	case reflect.Map:
		// Mapas (documentos aninhados) mapeiam bem para JSONB
		return "JSONB"
	default:
		// Tipos específicos do BSON/Mongo
		if t == reflect.TypeOf(primitive.ObjectID{}) {
			return "TEXT" // Armazenar ObjectID como string hexadecimal
		}
		if t == reflect.TypeOf(primitive.DateTime(0)) || t == reflect.TypeOf(time.Time{}) {
			// primitive.DateTime é int64, time.Time é struct.
			return "TIMESTAMPTZ" // Timestamp com fuso horário
		}
		if t == reflect.TypeOf(primitive.Decimal128{}) {
			return "NUMERIC" // Ou DECIMAL
		}
		if t == reflect.TypeOf(primitive.Binary{}) {
			return "BYTEA"
		}
		// Outros tipos BSON (Timestamp, Regex, JSCode, etc.) -> JSONB ou TEXT
		log.Printf("Aviso: Tipo Go não mapeado diretamente '%s'. Usando JSONB.", t.String())
		return "JSONB"
	}
}

// generateCreateTableSQL gera uma string SQL CREATE TABLE para PostgreSQL.
func generateCreateTableSQL(tableName string, schema map[string]string) string {
	// Sanitizar nome da tabela (simplesmente colocar entre aspas por enquanto)
	sanitizedTableName := fmt.Sprintf(`"%s"`, strings.ToLower(tableName))

	// Construir o SQL usando slice de strings e join, evitando caracteres de escape
	sqlParts := []string{
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS public.%s (", sanitizedTableName),
		"    id UUID PRIMARY KEY DEFAULT uuid_generate_v4()",
	}

	// Adicionar colunas do schema inferido (ordenadas por nome)
	fields := make([]string, 0, len(schema))
	for fieldName := range schema {
		fields = append(fields, fieldName)
	}
	sort.Strings(fields)

	for _, fieldName := range fields {
		sqlType := schema[fieldName]
		// Sanitizar nome da coluna (colocar entre aspas)
		sanitizedFieldName := fmt.Sprintf(`"%s"`, fieldName)
		sqlParts = append(sqlParts, fmt.Sprintf("    %s %s", sanitizedFieldName, sqlType))
	}

	// Adicionar colunas padrão de timestamp
	sqlParts = append(sqlParts, "    created_at TIMESTAMPTZ DEFAULT now()")
	sqlParts = append(sqlParts, "    updated_at TIMESTAMPTZ DEFAULT now()")
	sqlParts = append(sqlParts, ");")

	// Unir todas as partes com vírgulas e quebras de linha apropriadas
	sql := sqlParts[0] + "\n" + sqlParts[1]
	for i := 2; i < len(sqlParts)-1; i++ {
		sql += ",\n" + sqlParts[i]
	}
	sql += "\n" + sqlParts[len(sqlParts)-1]

	return sql
}

// migrateData insere documentos BSON em uma tabela PostgreSQL existente.
func migrateData(ctx context.Context, conn *pgx.Conn, tableName string, schema map[string]string, docs []bson.M) error {
	// Preparar lista de colunas para o INSERT (incluindo mongo_id, excluindo id, created_at, updated_at que têm defaults)
	// Ordenar para garantir consistência entre a query e os valores
	columns := make([]string, 0, len(schema))
	for colName := range schema {
		columns = append(columns, colName)
	}
	sort.Strings(columns)

	// Construir a parte inicial do INSERT statement
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString(fmt.Sprintf(`INSERT INTO public."%s" (`, tableName))
	placeholders := make([]string, 0, len(columns))
	quotedColumns := make([]string, 0, len(columns))

	paramIndex := 1
	for _, colName := range columns {
		quotedColumns = append(quotedColumns, fmt.Sprintf(`"%s"`, colName))
		placeholders = append(placeholders, fmt.Sprintf("$%d", paramIndex))
		paramIndex++
	}

	sqlBuilder.WriteString(strings.Join(quotedColumns, ", "))
	sqlBuilder.WriteString(") VALUES (")
	sqlBuilder.WriteString(strings.Join(placeholders, ", "))
	sqlBuilder.WriteString(")")

	insertSQL := sqlBuilder.String()
	// log.Println("DEBUG: INSERT SQL:", insertSQL) // Descomentar para depurar

	// Inserir documentos um por um (para simplificar, idealmente usar batch/copy)
	// TODO: Investigar pgx.CopyFrom para melhor performance
	insertedCount := 0
	for i, doc := range docs {
		values := make([]interface{}, 0, len(columns))
		for _, colName := range columns {
			rawValue, exists := doc[colName]
			if !exists || rawValue == nil {
				values = append(values, nil) // Mapeia campo ausente ou nulo para NULL
				continue
			}

			// Conversão de tipos BSON para SQL
			var sqlValue interface{}
			sqlType := schema[colName]

			switch specificVal := rawValue.(type) {
			case primitive.ObjectID:
				if colName == "mongo_id" {
					sqlValue = specificVal.Hex()
				} else {
					// ObjectID em outro campo? Tratar como texto.
					sqlValue = specificVal.Hex()
				}
			case primitive.DateTime:
				sqlValue = specificVal.Time()
			case time.Time: // Já é time.Time
				sqlValue = specificVal
			case primitive.Decimal128:
				// pgx pode lidar com Decimal128? Ou converter para string/float?
				// Convertendo para string por segurança
				sqlValue = specificVal.String()
			case primitive.Binary:
				sqlValue = specificVal.Data
			case map[string]interface{}, []interface{}: // Para JSONB
				if sqlType == "JSONB" {
					jsonBytes, err := json.Marshal(specificVal)
					if err != nil {
						log.Printf("Erro ao converter %s para JSON no doc %d: %v. Inserindo NULL.", colName, i, err)
						sqlValue = nil
					} else {
						sqlValue = jsonBytes
					}
				} else {
					// Tipo inesperado para coluna não JSONB
					log.Printf("Aviso: Tipo map/slice inesperado para coluna %s (tipo %s) no doc %d. Inserindo NULL.", colName, sqlType, i)
					sqlValue = nil
				}
			default:
				sqlValue = specificVal // Tipos básicos (string, int, float, bool) devem funcionar
			}
			values = append(values, sqlValue)
		}

		// Executar o INSERT
		_, err := conn.Exec(ctx, insertSQL, values...)
		if err != nil {
			// Logar erro mas continuar tentando os próximos documentos?
			log.Printf("ERRO ao inserir documento %d na tabela %s: %v\n  Valores: %v", i, tableName, err, values)
			// Poderíamos retornar o erro aqui para parar a migração desta tabela
			// return fmt.Errorf("erro ao inserir documento %d: %w", i, err)
		} else {
			insertedCount++
		}

		// Log de progresso (opcional, pode poluir muito)
		if (i+1)%100 == 0 {
			fmt.Printf("  ... %d / %d documentos inseridos em %s ...\n", i+1, len(docs), tableName)
		}
	}
	fmt.Printf("  %d / %d documentos inseridos com sucesso em %s.\n", insertedCount, len(docs), tableName)
	if insertedCount != len(docs) {
		log.Printf("Aviso: %d erros ocorreram durante a inserção na tabela %s.", len(docs)-insertedCount, tableName)
	}

	return nil // Retorna nil mesmo se houver erros individuais (para continuar com outras tabelas)
}
