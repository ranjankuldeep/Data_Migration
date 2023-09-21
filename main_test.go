package main

import "testing"

func TestGenerateInsertSQL(t *testing.T) {

	tests := []struct {
		name    string
		oplog   string
		want    string
		wantErr bool
	}{
		{name: "Insert Operation", oplog: "", want: "", wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateInsertSQL(tt.oplog)
			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateInsertSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GenerateInsertSQL() = %v, want %v", got, tt.want)
			}
		})
	}
}
