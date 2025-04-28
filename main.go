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

// Função para sanitizar nomes de tabelas, substituindo caracteres inválidos
func sanitizeTableName(name string) string {
	// Substituir pontos por sublinhados
	name = strings.ReplaceAll(name, ".", "_")

	// Colocar em minúsculas para consistência
	name = strings.ToLower(name)

	// Substituir outros caracteres que podem causar problemas
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "/", "_")

	return name
}

func main() {
	// Configuração via flags
	config := Config{}
	flag.StringVar(&config.DataPath, "path", "", "Caminho para os arquivos BSON/JSON exportados do MongoDB")
	flag.BoolVar(&config.DropExisting, "drop", false, "Apagar tabelas existentes antes de criar novas")
	collectionsFlag := flag.String("collections", "", "Lista de collections para migrar, separadas por vírgula (opcional)")
	skipFlag := flag.String("skip", "", "Lista de collections para ignorar, separadas por vírgula (opcional)")
	envFile := flag.String("env", ".env", "Caminho para o arquivo .env")
	debugFlag := flag.Bool("debug", false, "Ativar modo de depuração com mais informações")
	batchSizeFlag := flag.Int("batch", 100, "Tamanho do lote para inserções em massa (padrão: 100)")
	flag.Parse()

	if config.DataPath == "" {
		log.Fatal("Erro: O caminho para os arquivos exportados do MongoDB é obrigatório. Use a flag -path")
	}

	// Definir o nível de log com base na flag de depuração
	if *debugFlag {
		log.Println("Modo de depuração ativado - exibindo informações detalhadas")
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

		// Sanitizar o nome da tabela para PostgreSQL
		sanitizedTableName := sanitizeTableName(collectionName)

		fmt.Printf("\n--- Processando Collection: %s → Tabela: %s (Arquivo: %s) ---\n",
			collectionName, sanitizedTableName, fileName)

		// Verificar se deve pular esta collection
		if shouldSkip(collectionName, config.OnlyCollections, config.SkipCollections) {
			fmt.Printf("Pulando collection: %s (filtrada por configuração)\n", collectionName)
			continue
		}

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

		// Na parte onde chamamos migrateData, modificar para:
		err = migrateData(ctx, conn, sanitizedTableName, inferredSchema, docs, *batchSizeFlag)
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
	// Para debugging e melhor inferência
	fieldExamples := make(map[string]interface{})

	for _, doc := range docs {
		for key, value := range doc {
			if fieldTypes[key] == nil {
				fieldTypes[key] = make(map[reflect.Type]int)
			}

			// Guardar um exemplo não-nulo do valor para melhor inferência
			if value != nil && fieldExamples[key] == nil {
				fieldExamples[key] = value
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

		// Mostrar todos os tipos encontrados para este campo (para debugging)
		typeNames := make([]string, 0, len(typeCounts))
		for typ, count := range typeCounts {
			typeName := "nil"
			if typ != reflect.TypeOf((*interface{})(nil)).Elem() {
				typeName = typ.String()
			}
			typeNames = append(typeNames, fmt.Sprintf("%s:%d", typeName, count))

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

		// Imprimir tipos para diagnóstico em caso de múltiplos tipos
		if multipleTypes && len(typeNames) > 1 {
			log.Printf("Campo '%s' tem múltiplos tipos: %v", fieldName, strings.Join(typeNames, ", "))
		}

		// Se só vimos nil, ou não vimos nenhum tipo (campo sempre nil?)
		if dominantType == nil {
			// Vamos assumir TEXT como um palpite seguro se só vimos nulos.
			// Ou poderíamos omitir a coluna, mas TEXT é mais flexível.
			inferredSQLSchema[fieldName] = "TEXT" // Ou talvez JSONB? Ou omitir?
			log.Printf("Aviso: Campo '%s' parece ser sempre nulo ou ausente. Mapeando para TEXT.", fieldName)
			continue
		}

		// Caso especial: timestamps e dates do MongoDB
		example := fieldExamples[fieldName]
		switch example.(type) {
		case primitive.DateTime, time.Time:
			// Sempre mapear datas para TIMESTAMPTZ independentemente da prevalência
			inferredSQLSchema[fieldName] = "TIMESTAMPTZ"
			log.Printf("Campo '%s' contém timestamp. Forçando tipo TIMESTAMPTZ.", fieldName)
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
				log.Printf("Aviso: Campo '%s' tem múltiplos tipos. Mapeando para JSONB.", fieldName)
			}
		} else {
			// Mapear o tipo dominante para SQL
			inferredSQLSchema[fieldName] = mapGoTypeToSQL(dominantType)
		}
	}

	// Log do schema completo para cada tabela
	var schemaDesc strings.Builder
	schemaDesc.WriteString("Schema inferido:\n")

	fields := make([]string, 0, len(inferredSQLSchema))
	for fieldName := range inferredSQLSchema {
		fields = append(fields, fieldName)
	}
	sort.Strings(fields)

	for _, field := range fields {
		sqlType := inferredSQLSchema[field]
		schemaDesc.WriteString(fmt.Sprintf("  - %s: %s\n", field, sqlType))
	}

	log.Print(schemaDesc.String())

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
func migrateData(ctx context.Context, conn *pgx.Conn, tableName string, schema map[string]string, docs []bson.M, batchSize int) error {
	fmt.Printf("Iniciando migração para tabela %s (%d documentos)...\n", tableName, len(docs))

	// Verificar schema atual da tabela para garantir compatibilidade
	rows, err := conn.Query(ctx, `
		SELECT column_name, data_type, udt_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
	`, tableName)
	if err != nil {
		log.Printf("Erro ao consultar schema da tabela %s: %v", tableName, err)
	} else {
		defer rows.Close()

		pgSchema := make(map[string]string)
		for rows.Next() {
			var columnName, dataType, udtName string
			if err := rows.Scan(&columnName, &dataType, &udtName); err != nil {
				log.Printf("Erro ao ler schema: %v", err)
				continue
			}
			pgSchema[columnName] = dataType
		}

		// Log para comparar schema inferido vs. real
		log.Printf("Schema real da tabela %s no PostgreSQL:", tableName)
		for col, typ := range pgSchema {
			log.Printf("  - %s: %s", col, typ)
		}
	}

	// Preparar lista de colunas para o INSERT (incluindo mongo_id, excluindo id, created_at, updated_at que têm defaults)
	// Ordenar para garantir consistência entre a query e os valores
	columns := make([]string, 0, len(schema))
	for colName := range schema {
		columns = append(columns, colName)
	}
	sort.Strings(columns)

	if len(columns) == 0 {
		log.Printf("AVISO: Nenhuma coluna encontrada para migrar na tabela %s", tableName)
		return fmt.Errorf("nenhuma coluna para migrar")
	}

	// Debug: mostrar as colunas que serão migradas
	fmt.Printf("Colunas a serem migradas: %v\n", columns)

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
	fmt.Printf("SQL de inserção: %s\n", insertSQL) // Log para debug

	// Verificar se a tabela metadata tem apenas um documento
	isMetadataTable := strings.Contains(tableName, "_metadata")
	if isMetadataTable && len(docs) == 1 {
		fmt.Println("Tabela de metadados detectada. Inserindo documento único...")
	}

	// Configuração do tamanho do lote
	// Se for uma collection pequena, fazer inserção um por um
	if len(docs) <= 10 {
		batchSize = 1
	}

	fmt.Printf("Usando tamanho de lote: %d para %d documentos\n", batchSize, len(docs))

	// Contadores e estatísticas
	insertedCount := 0
	errorCount := 0
	batchCount := 0
	startTime := time.Now()

	// Processar documentos em lotes
	for i := 0; i < len(docs); i += batchSize {
		batchCount++
		end := i + batchSize
		if end > len(docs) {
			end = len(docs)
		}

		processingBatch := docs[i:end]
		batchStartTime := time.Now()
		fmt.Printf("  Lote #%d: Processando documentos %d-%d de %d...\n", batchCount, i+1, end, len(docs))

		// Iniciar uma transação para o lote inteiro
		tx, err := conn.Begin(ctx)
		if err != nil {
			log.Printf("ERRO ao iniciar transação para lote #%d: %v", batchCount, err)
			errorCount += len(processingBatch)
			continue
		}

		batchSuccessCount := 0
		batchErrors := 0

		// Processar cada documento no lote
		for docIndex, doc := range processingBatch {
			// Modificação: se _id existe, copiar para mongo_id
			if id, hasID := doc["_id"]; hasID {
				switch v := id.(type) {
				case primitive.ObjectID:
					doc["mongo_id"] = v.Hex()
				default:
					// Para outros tipos, converter para string
					doc["mongo_id"] = fmt.Sprintf("%v", v)
				}
			}

			values := make([]interface{}, 0, len(columns))
			for _, colName := range columns {
				var rawValue interface{}
				var exists bool

				// Caso especial para mongo_id que pode vir do _id
				if colName == "mongo_id" {
					rawValue, exists = doc["mongo_id"]
					if !exists {
						if id, hasID := doc["_id"]; hasID {
							switch v := id.(type) {
							case primitive.ObjectID:
								rawValue = v.Hex()
								exists = true
							default:
								rawValue = fmt.Sprintf("%v", v)
								exists = true
							}
						}
					}
				} else {
					rawValue, exists = doc[colName]
				}

				if !exists || rawValue == nil {
					values = append(values, nil) // Mapeia campo ausente ou nulo para NULL
					continue
				}

				// Conversão de tipos BSON para SQL
				var sqlValue interface{}
				sqlType := schema[colName]

				// Verificar compatibilidade de tipos
				if sqlType == "BIGINT" || sqlType == "INTEGER" {
					// Convertemos timestamps para números se necessário
					switch v := rawValue.(type) {
					case primitive.DateTime:
						// Converter para int64 se for para coluna numérica
						sqlValue = int64(v)
					case time.Time:
						// Converter para Unix timestamp se for para coluna numérica
						sqlValue = v.Unix()
					default:
						// Usar o valor como está se for numérico
						sqlValue = v
					}
					values = append(values, sqlValue)
					continue
				}

				if sqlType == "TIMESTAMPTZ" {
					// Garantir que temos timestamp para colunas de data
					switch v := rawValue.(type) {
					case primitive.DateTime:
						sqlValue = v.Time()
					case int64:
						// Converter int64 para timestamp se necessário
						sqlValue = time.Unix(0, v*int64(time.Millisecond))
					case time.Time:
						sqlValue = v
					default:
						// Não conseguimos converter para timestamp
						log.Printf("Aviso: Valor incompatível %T para coluna TIMESTAMPTZ %s. Inserindo NULL.",
							v, colName)
						sqlValue = nil
					}
					values = append(values, sqlValue)
					continue
				}

				// Tratamento padrão para outros tipos
				switch specificVal := rawValue.(type) {
				case primitive.ObjectID:
					sqlValue = specificVal.Hex()
				case primitive.DateTime:
					sqlValue = specificVal.Time()
				case time.Time: // Já é time.Time
					sqlValue = specificVal
				case primitive.Decimal128:
					// Convertendo para string por segurança
					sqlValue = specificVal.String()
				case primitive.Binary:
					sqlValue = specificVal.Data
				case map[string]interface{}, []interface{}: // Para JSONB
					if sqlType == "JSONB" {
						jsonBytes, err := json.Marshal(specificVal)
						if err != nil {
							log.Printf("Erro ao converter para JSON. Inserindo NULL.")
							sqlValue = nil
						} else {
							sqlValue = string(jsonBytes) // Convertendo para string para JSONB
						}
					} else {
						log.Printf("Aviso: Tipo map/slice inesperado para coluna %s (tipo %s). Convertendo para JSONB.", colName, sqlType)
						jsonBytes, err := json.Marshal(specificVal)
						if err != nil {
							sqlValue = nil
						} else {
							sqlValue = string(jsonBytes)
						}
					}
				default:
					sqlValue = specificVal // Tipos básicos (string, int, float, bool) devem funcionar
				}
				values = append(values, sqlValue)
			}

			// Executar o INSERT dentro da transação
			_, err := tx.Exec(ctx, insertSQL, values...)
			if err != nil {
				batchErrors++

				// Se estamos no modo de depuração ou este é um dos primeiros erros, mostrar detalhes
				if batchErrors <= 3 || batchErrors == len(docs)/10 {
					log.Printf("ERRO ao inserir documento #%d (global #%d) na tabela %s: %v",
						docIndex, i+docIndex+1, tableName, err)

					// Mostrar os primeiros valores para depuração
					debugValues := "["
					for j, v := range values {
						if j < 5 {
							if v == nil {
								debugValues += "NULL, "
							} else {
								debugValues += fmt.Sprintf("%T(%v), ", v, v)
							}
						} else {
							debugValues += "..."
							break
						}
					}
					debugValues += "]"
					log.Printf("  Valores (parcial): %s", debugValues)
				}
			} else {
				batchSuccessCount++
			}
		}

		// Finalizar a transação
		if batchErrors == 0 {
			// Nenhum erro, commit da transação
			if err := tx.Commit(ctx); err != nil {
				log.Printf("ERRO ao fazer commit da transação para lote #%d: %v", batchCount, err)
				errorCount += len(processingBatch)
			} else {
				insertedCount += batchSuccessCount

				// Calcular e exibir estatísticas deste lote
				batchDuration := time.Since(batchStartTime)
				docsPerSecond := float64(batchSuccessCount) / batchDuration.Seconds()

				fmt.Printf("  Lote #%d: Inseridos %d documentos em %.2f segundos (%.2f docs/seg)\n",
					batchCount, batchSuccessCount, batchDuration.Seconds(), docsPerSecond)
			}
		} else {
			// Houve erros, rollback da transação
			tx.Rollback(ctx)
			log.Printf("Lote #%d: Rollback da transação devido a %d erros.", batchCount, batchErrors)
			errorCount += batchErrors

			// Se o tamanho do lote é grande e estamos tendo muitos erros,
			// tentar reduzir o tamanho do lote para as próximas iterações
			if batchSize > 10 && batchErrors > batchSize/2 {
				newBatchSize := batchSize / 2
				log.Printf("Reduzindo tamanho do lote de %d para %d devido a muitos erros", batchSize, newBatchSize)
				batchSize = newBatchSize
			}

			// Se o lote for grande e falhar completamente, tentar inserir documento por documento
			if batchSize > 1 && batchSuccessCount == 0 {
				log.Printf("Tentando inserção documento por documento para o lote #%d...", batchCount)

				// Tentar inserir cada documento individualmente fora da transação
				for _, docToInsert := range processingBatch {
					// Copiar o mesmo processo de preparação usado acima
					if id, hasID := docToInsert["_id"]; hasID {
						switch v := id.(type) {
						case primitive.ObjectID:
							docToInsert["mongo_id"] = v.Hex()
						default:
							docToInsert["mongo_id"] = fmt.Sprintf("%v", v)
						}
					}

					individualValues := make([]interface{}, 0, len(columns))
					for _, colName := range columns {
						var rawValue interface{}
						var exists bool

						if colName == "mongo_id" {
							rawValue, exists = docToInsert["mongo_id"]
							if !exists {
								if id, hasID := docToInsert["_id"]; hasID {
									switch v := id.(type) {
									case primitive.ObjectID:
										rawValue = v.Hex()
										exists = true
									default:
										rawValue = fmt.Sprintf("%v", v)
										exists = true
									}
								}
							}
						} else {
							rawValue, exists = docToInsert[colName]
						}

						if !exists || rawValue == nil {
							individualValues = append(individualValues, nil)
							continue
						}

						// Conversão de tipos como acima
						var sqlValue interface{}
						sqlType := schema[colName]

						// Verificar compatibilidade de tipos
						if sqlType == "BIGINT" || sqlType == "INTEGER" {
							switch v := rawValue.(type) {
							case primitive.DateTime:
								sqlValue = int64(v)
							case time.Time:
								sqlValue = v.Unix()
							default:
								sqlValue = v
							}
							individualValues = append(individualValues, sqlValue)
							continue
						}

						if sqlType == "TIMESTAMPTZ" {
							switch v := rawValue.(type) {
							case primitive.DateTime:
								sqlValue = v.Time()
							case int64:
								sqlValue = time.Unix(0, v*int64(time.Millisecond))
							case time.Time:
								sqlValue = v
							default:
								sqlValue = nil
							}
							individualValues = append(individualValues, sqlValue)
							continue
						}

						// Tratamento padrão
						switch specificVal := rawValue.(type) {
						case primitive.ObjectID:
							sqlValue = specificVal.Hex()
						case primitive.DateTime:
							sqlValue = specificVal.Time()
						case time.Time:
							sqlValue = specificVal
						case primitive.Decimal128:
							sqlValue = specificVal.String()
						case primitive.Binary:
							sqlValue = specificVal.Data
						case map[string]interface{}, []interface{}:
							if sqlType == "JSONB" {
								jsonBytes, err := json.Marshal(specificVal)
								if err != nil {
									sqlValue = nil
								} else {
									sqlValue = string(jsonBytes)
								}
							} else {
								jsonBytes, err := json.Marshal(specificVal)
								if err != nil {
									sqlValue = nil
								} else {
									sqlValue = string(jsonBytes)
								}
							}
						default:
							sqlValue = specificVal
						}
						individualValues = append(individualValues, sqlValue)
					}

					// Tentar inserção individual
					_, err := conn.Exec(ctx, insertSQL, individualValues...)
					if err == nil {
						insertedCount++
					}
				}
			}
		}

		// Mostrar progresso geral
		percentComplete := float64(i+len(processingBatch)) / float64(len(docs)) * 100
		elapsedTime := time.Since(startTime)
		estimatedTotal := elapsedTime.Seconds() / (float64(i+len(processingBatch)) / float64(len(docs)))
		remainingTime := estimatedTotal - elapsedTime.Seconds()

		fmt.Printf("  Progresso: %.1f%% (%d/%d) | Tempo estimado restante: %.1f segundos\n",
			percentComplete, i+len(processingBatch), len(docs), remainingTime)
	}

	// Se não houve nenhuma inserção e temos registros, tentar fazer um Insert mínimo para metadados
	if insertedCount == 0 && len(docs) > 0 && isMetadataTable {
		log.Printf("Tentando inserção simplificada para tabela de metadados %s", tableName)

		// Para metadados, tentar inserir apenas o _id como fallback
		if id, hasID := docs[0]["_id"]; hasID {
			var idStr string
			switch v := id.(type) {
			case primitive.ObjectID:
				idStr = v.Hex()
			default:
				idStr = fmt.Sprintf("%v", v)
			}

			_, err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO public."%s" (mongo_id) VALUES ($1)`, tableName), idStr)
			if err == nil {
				insertedCount = 1
				log.Printf("Inserção simplificada bem-sucedida para %s", tableName)
			} else {
				log.Printf("Falha na inserção simplificada: %v", err)
			}
		}
	}

	// Mostrar estatísticas finais
	totalTime := time.Since(startTime)
	avgDocsPerSecond := float64(insertedCount) / totalTime.Seconds()

	fmt.Printf("\nMigração concluída para %s:\n", tableName)
	fmt.Printf("  Duração total: %.2f segundos\n", totalTime.Seconds())
	fmt.Printf("  Documentos inseridos: %d / %d (%.1f%%)\n",
		insertedCount, len(docs), float64(insertedCount)/float64(len(docs))*100)
	fmt.Printf("  Taxa média: %.2f documentos/segundo\n", avgDocsPerSecond)

	if insertedCount != len(docs) {
		log.Printf("Aviso: %d erros ocorreram durante a inserção na tabela %s.", len(docs)-insertedCount, tableName)
		if errorCount > 0 {
			return fmt.Errorf("ocorreram %d erros durante a migração", errorCount)
		}
	}

	return nil
}
