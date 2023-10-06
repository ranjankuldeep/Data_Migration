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
	//here interface{} is any type so o is type of kind of javascript object
}

func GenerateInsertSQL(oplog string) (string, error) {
	var oplogObj OplogEntry
	// []byte(var) converts the var string to array of byte
	if err := json.Unmarshal([]byte(oplog), &oplogObj); err != nil {
		return "", nil
	}
	columnNames := make([]string, 0, len(oplogObj.O))

	columnValues := make([]string, 0, len(oplogObj.O))
	for columnName := range oplogObj.O {
		columnNames = append(columnNames, columnName)
	}
	sort.Strings(columnNames)

	for _, columnName := range columnNames {
		columnValues = append(columnValues, getColumnValues(oplogObj.O[columnName]))
	}

	switch oplogObj.Op {
	case "i":
		sql := fmt.Sprintf("INSERT INTO " + oplogObj.NS)
		sql = fmt.Sprintf("%s (%s) VALUES (%s);", sql, strings.Join(columnNames, ", "), strings.Join(columnValues, ", "))
		return sql, nil

	}

	return "", nil
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
