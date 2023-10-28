package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func main() {
	fmt.Println("Hello World")
}

// unmarshal the json string following the below structuture
type OplogEntry struct {
	Op string                 `json:"op"`
	NS string                 `json:"ns"`
	O  map[string]interface{} `json:"o"`
	O2 map[string]interface{} `json:"o2"`
	//here interface{} is any type so o is type of kind of javascript object
}

func generateCreateSchemaSQL(oplogObj OplogEntry, sqls []string) []string {
	nsParts := strings.Split(oplogObj.NS, ".")
	sqls = append(sqls, fmt.Sprintf("CREATE SCHEMA %s;", nsParts[0]))
	return sqls
}
func getColumnSQLDataType(columnName string, value interface{}) string {
	colDataType := ""
	switch value.(type) {
	case int, int8, int16, int32, int64:
		colDataType = "INTEGER"
	case float32, float64:
		colDataType = "FLOAT"
	case bool:
		colDataType = "BOOLEAN"
	default:
		colDataType = "VARCHAR(255)"
	}
	if columnName == "_id" {
		colDataType += " PRIMARY KEY"
	}
	return colDataType
}
func generateCreateTabelSQL(oplogObj OplogEntry) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (", oplogObj.NS))

	columnNames := getColumnNames(oplogObj.O)

	sep := ""
	for _, columnName := range columnNames {
		value := oplogObj.O[columnName]
		colDataType := getColumnSQLDataType(columnName, value)

		sb.WriteString(fmt.Sprintf("%s%s %s", sep, columnName, colDataType))
		sep = ", "
	}
	sb.WriteString(");")
	return sb.String()

}
func GenerateSQL(oplog string) ([]string, error) {
	var oplogObj OplogEntry
	// []byte(var) converts the var string to array of byte
	sqls := []string{}

	if err := json.Unmarshal([]byte(oplog), &oplogObj); err != nil {
		return sqls, err
	}
	switch oplogObj.Op {
	case "i":
		//create schema
		sqls = generateCreateSchemaSQL(oplogObj, sqls)
		//create table
		sqls = append(sqls, generateCreateTabelSQL(oplogObj))
		sql, err := generateInsertSQL(oplogObj)
		if err != nil {
			return sqls, err
		}
		sqls = append(sqls, sql)
	case "u":
		sql, err := generateUpdateSQL(oplogObj)
		if err != nil {
			return sqls, err
		}
		sqls = append(sqls, sql)
	case "d":
		sql, err := generateDeleteSQL(oplogObj)
		if err != nil {
			return sqls, err
		}
		sqls = append(sqls, sql)
	}

	return sqls, nil
}

func generateInsertSQL(oplogObj OplogEntry) (string, error) {
	//INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);
	switch oplogObj.Op {
	case "i":
		columnNames := make([]string, 0, len(oplogObj.O))
		columnValues := make([]string, 0, len(oplogObj.O))

		for columnName := range oplogObj.O {
			columnNames = append(columnNames, columnName)
		}

		sort.Strings(columnNames)

		for _, columnName := range columnNames {
			columnValues = append(columnValues, getColumnValues(oplogObj.O[columnName]))
		}
		sql := fmt.Sprintf("INSERT INTO " + oplogObj.NS)
		sql = fmt.Sprintf("%s (%s) VALUES (%s);", sql, strings.Join(columnNames, ", "), strings.Join(columnValues, ", "))
		return sql, nil
	}
	return "", nil
}
func generateUpdateSQL(oplogObj OplogEntry) (string, error) {
	//UPDATE test.student SET is_graduated = true WHERE _id = '635b79e231d82a8ab1de863b';
	switch oplogObj.Op {
	case "u":
		sql := fmt.Sprintf("UPDATE %s SET", oplogObj.NS)
		diffMap, ok := oplogObj.O["diff"].(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("invalid oplog")
		}
		if setMap, ok := diffMap["u"].(map[string]interface{}); ok {
			columnValues := make([]string, 0, len(setMap))
			for columnName, value := range setMap {
				columnValues = append(columnValues, fmt.Sprintf("%s = %s", columnName, getColumnValues(value)))
			}
			sort.Strings(columnValues)
			sql = fmt.Sprintf("%s %s", sql, strings.Join(columnValues, ", "))
		} else if unsetMap, ok := diffMap["d"].(map[string]interface{}); ok {
			columnValues := make([]string, 0, len(unsetMap))
			for columnName := range unsetMap {
				columnValues = append(columnValues, fmt.Sprintf("%s = NULL", columnName))
			}
			sort.Strings(columnValues)
			sql = fmt.Sprintf("%s %s", sql, strings.Join(columnValues, ", "))
		} else {
			return "", fmt.Errorf("invalid oplog")
		}
		whereClauseCols := make([]string, 0, len(oplogObj.O2))
		for columnName, value := range oplogObj.O2 {
			whereClauseCols = append(whereClauseCols, fmt.Sprintf("%s = %s", columnName, getColumnValues(value)))
		}
		sql = fmt.Sprintf("%s WHERE %s;", sql, strings.Join(whereClauseCols, " AND "))
		return sql, nil
	}
	return "", fmt.Errorf("invalid oplog")
}
func generateDeleteSQL(oplogObj OplogEntry) (string, error) {
	//DELETE FROM test.student WHERE _id = '635b79e231d82a8ab1de863b';
	switch oplogObj.Op {
	case "d":
		sql := fmt.Sprintf("DELETE FROM %s", oplogObj.NS)
		whereClauseCols := make([]string, 0, len(oplogObj.O))
		for columnName, value := range oplogObj.O {
			whereClauseCols = append(whereClauseCols, fmt.Sprintf("%s = %s", columnName, getColumnValues(value)))
		}
		sql = fmt.Sprintf("%s WHERE %s;", sql, strings.Join(whereClauseCols, " AND "))
		return sql, nil
	}
	return "", fmt.Errorf("invalid oplog")
}
func getColumnValues(value interface{}) string {
	switch value.(type) {
	case int, int8, int16, int32, int64, float64:
		return fmt.Sprintf("%v", value)
	case bool:
		return fmt.Sprintf("%t", value)
	default:
		return fmt.Sprintf("'%v'", value)
	}
}
func getColumnNames(data map[string]interface{}) []string {
	columnNames := make([]string, 0, len(data))
	for columnName, _ := range data {
		columnNames = append(columnNames, columnName)
	}
	sort.Strings(columnNames)
	return columnNames
}
