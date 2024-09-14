package main

import (
	"archive/zip"
	"database/sql"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"strings"

	_ "github.com/lib/pq"
)

func readFromZIP() {
	// Command line arguments
	zipFilePath := flag.String("zip", "/Users/evgeny.lugin/go/src/scripts_el/5830e3f8-8103-4a85-9bd4-5f71bdfb7c5f-DKIM.zip", "Path to zip file")
	dbHost := flag.String("host", "localhost", "Database host")
	dbPort := flag.String("port", "5432", "Database port")
	dbUser := flag.String("user", "postgres", "Database user")
	dbName := flag.String("dbname", "postgres", "Database name")
	schemas := flag.String("schemas", "identity,chs", "Comma-separated list of schemas")
	tables := flag.String("tables", "identity, active_dns_record, selector_relation, selector_domain, selector_issue", "Comma-separated list of tables")
	flag.Parse()

	log.Println(fmt.Sprintf("Got config %s %s %s %s %s", *zipFilePath, *dbHost, *dbPort, *dbUser, *dbName))
	// Validate arguments
	if *zipFilePath == "" || *dbUser == "" || *dbName == "" {
		log.Fatal("zip, user, and dbname arguments are required")
	}

	// Connect to database
	connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s search_path=cis_test sslmode=disable",
		*dbHost, *dbPort, *dbUser, *dbName)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Open zip file
	r, err := zip.OpenReader(*zipFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	log.Println("Reading files...")
	// Process each file in the zip
	for _, f := range r.File {
		if err := processFile(f, db, *schemas, *tables); err != nil {
			log.Printf("Error processing file %s: %v\n", f.Name, err)
		}

	}
	log.Println("Finish reading files")
}

func processFile(f *zip.File, db *sql.DB, schemas string, tables string) error {
	// Check if file matches specified schemas and tables
	fileNameParts := strings.Split(strings.TrimPrefix(f.Name, "dump/"), ".")
	if !fileMatches(fileNameParts, schemas, tables) {
		return nil // Skip file
	}
	log.Println(fmt.Sprintf("File match %s", f.Name))

	// Open the file
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	reader := csv.NewReader(rc)

	// Read the first line for column names
	columns, err := reader.Read()
	if err == io.EOF {
		return nil
	}
	//log.Println("columns: ", columns)

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		values := parseCSVLine(row)
		if len(columns) != len(values) {
			log.Println(values)
			return errors.New(fmt.Sprintf("columns and values number length mismatch %d cols and %d vals", len(columns), len(values)))
		}

		query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s);", "cis_test", fileNameParts[1], strings.Join(columns, ", "), strings.Join(values, ", "))

		if _, err := db.Exec(query); err != nil {
			if strings.Contains(err.Error(), "does not exist") && strings.Contains(err.Error(), "pq: relation") {
				log.Println(err)
				continue
			}
			return err
		}
	}

	return nil
}

func parseCSVLine(line []string) []string {
	for i, field := range line {
		if field == "\\N" {
			line[i] = "NULL"
		} else {
			line[i] = "'" + strings.Replace(field, "'", "''", -1) + "'"
		}
	}
	return line
}

func fileMatches(fileNameParts []string, schemas, tables string) bool {
	if len(fileNameParts) != 3 || fileNameParts[2] != "csv" {
		return false
	}
	schema, table := fileNameParts[0], fileNameParts[1]

	if schemas != "" && !strings.Contains(schemas, schema) {
		return false
	}
	if tables != "" && !strings.Contains(tables, table) {
		return false
	}
	return true
}
