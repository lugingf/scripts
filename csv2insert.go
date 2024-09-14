package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	Schema        string
	Path          string
	IgnoreDeleted bool
	Tables        []string
}

type TableData struct {
	Columns []string
	Values  [][]string
}

type Processor struct {
	IgnoreDeleted bool
}

func csv2insert() {
	config := Config{
		Schema:        "identity",
		Path:          "/Users/evgeny.lugin/Downloads/dump",
		IgnoreDeleted: true,
		Tables: []string{
			"identity",
			"selector_certificate",
			//"active_dns_record",
			"selector_relation",
			"selector_domain",
			//"selector_host",
			"selector_relation_issue",
			"selector_issue",
			"active_cert_record",
		},
	}

	files, err := ioutil.ReadDir(config.Path)
	if err != nil {
		log.Fatalf("Failed to read directory: %v", err)
	}

	tableDataMap := make(map[string]*TableData)

	processor := Processor{IgnoreDeleted: config.IgnoreDeleted}

	for _, f := range files {
		for _, tablename := range config.Tables {
			fName := fmt.Sprintf("%s.%s.csv", config.Schema, tablename)
			if f.Name() != fName {
				continue
			}
			fmt.Println("processing: ", fName)
			data, err := processor.processCSVFile(filepath.Join(config.Path, f.Name()))
			if err != nil {
				log.Fatalf("Failed to process file %s: %v", f.Name(), err)
			}

			if data != nil {
				if tableData, ok := tableDataMap[tablename]; ok {
					fmt.Println(tablename, "is in tableDataMap")
					tableData.Values = append(tableData.Values, data.Values...)
				} else {
					fmt.Println(tablename, "is NOT in tableDataMap yet")
					tableDataMap[tablename] = data
				}
				fmt.Println(tablename, "data is nil")
			}
			fmt.Println(tablename, " processed")
			fmt.Println(tablename, "tableDataMap ready")
		}
	}
	fmt.Println("All files processed")

	finalSQL := processor.generateInsertSQL(tableDataMap)
	fmt.Println("finalSQL ready. Len is ", len(finalSQL))

	file, err := os.Create("output.sql")
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer file.Close()

	_, err = file.WriteString(finalSQL)
	if err != nil {
		log.Fatalf("Failed to write to output file: %v", err)
	}

	fmt.Println("SQL written to output.sql")
}

func (p Processor) processValue(value string) string {
	// Handling NULL value
	if value == "\\N" {
		return "NULL"
	}

	// Convert CSV double-double-quotes to regular double quotes for JSON values
	if (strings.HasPrefix(value, `"[`) && strings.HasSuffix(value, `]"`)) || strings.HasPrefix(value, "{") || strings.HasPrefix(value, "[") {
		value = strings.Trim(value, "\"")
		value = strings.ReplaceAll(value, `""`, `"`)
	}

	// Escape single quotes for SQL
	value = strings.ReplaceAll(value, "'", "''")

	return "'" + value + "'"
}

func (p Processor) processCSVFile(filename string) (*TableData, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	r := csv.NewReader(file)

	// Reading headers
	fmt.Println("Reading columns of ", filename)
	columns, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("reading headers failed: %v", err)
	}

	// Ignore deleted records
	dateDeletedIndex := -1
	if p.IgnoreDeleted {
		for i, col := range columns {
			if col == "date_deleted" {
				dateDeletedIndex = i
				break
			}
		}
	}

	var valuesList [][]string
	fmt.Println("Reading lines of ", filename)
	for {
		values, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading row failed: %v", err)
		}

		if p.IgnoreDeleted {
			if dateDeletedIndex != -1 && values[dateDeletedIndex] != "NULL" && values[dateDeletedIndex] != "\\N" {
				continue
			}
		}

		for i, value := range values {
			values[i] = p.processValue(value)
		}
		valuesList = append(valuesList, values)
	}

	fmt.Println("Return table data ", filename, "Rows count:", len(valuesList))
	return &TableData{
		Columns: columns,
		Values:  valuesList,
	}, nil
}

func (p Processor) generateInsertSQL(tableDataMap map[string]*TableData) string {
	var combinedSQL strings.Builder

	for tablename, data := range tableDataMap {
		if len(data.Values) > 0 {
			combinedSQL.WriteString(fmt.Sprintf("INSERT INTO cis_test.%s (%s) VALUES", tablename, strings.Join(data.Columns, ",")))
			for _, rowValues := range data.Values {
				combinedSQL.WriteString(fmt.Sprintf("\n\t(%s),", strings.Join(escapeString(rowValues), ",")))
			}
			combinedSQLStr := combinedSQL.String()
			combinedSQL.Reset()
			combinedSQL.WriteString(strings.TrimSuffix(combinedSQLStr, ","))
			combinedSQL.WriteString(";\n\n")
		}
	}

	return combinedSQL.String()
}

func escapeString(str []string) []string {
	result := make([]string, 0, len(str))
	for _, row := range str {
		result = append(result, strings.ReplaceAll(strings.ReplaceAll(row, "\\t", " "), "\\", "\\\\"))
	}
	return result
}
