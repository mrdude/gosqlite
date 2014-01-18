// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	"testing"

	"github.com/bmizerany/assert"
	. "github.com/gwenn/gosqlite"
)

func createIndex(db *Conn, t *testing.T) {
	err := db.Exec("DROP INDEX IF EXISTS test_index;" +
		"CREATE INDEX test_index on test(a_string)")
	checkNoError(t, err, "error creating index: %s")
}

func TestDatabases(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	databases, err := db.Databases()
	checkNoError(t, err, "error looking for databases: %s")
	if len(databases) != 1 {
		t.Errorf("Expected one database but got %d\n", len(databases))
	}
	if _, ok := databases["main"]; !ok {
		t.Errorf("Expected 'main' database\n")
	}
}

func TestTables(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	tables, err := db.Tables("")
	checkNoError(t, err, "error looking for tables: %s")
	assert.Equal(t, 0, len(tables), "table count")
	createTable(db, t)
	tables, err = db.Tables("main")
	checkNoError(t, err, "error looking for tables: %s")
	assert.Equal(t, 1, len(tables), "table count")
	assert.Equal(t, "test", tables[0], "table name")
}

func TestColumns(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	createTable(db, t)

	columns, err := db.Columns("", "test")
	checkNoError(t, err, "error listing columns: %s")
	if len(columns) != 4 {
		t.Fatalf("Expected 4 columns <> %d", len(columns))
	}
	column := columns[2]
	assert.Equal(t, "int_num", column.Name, "column name")

	columns, err = db.Columns("main", "test")
	checkNoError(t, err, "error listing columns: %s")
}

func TestColumn(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	createTable(db, t)

	column, err := db.Column("", "test", "id")
	checkNoError(t, err, "error getting column metadata: %s")
	assert.Equal(t, "id", column.Name, "column name")
	assert.Equal(t, 1, column.Pk, "primary key index")
	assert.T(t, !column.Autoinc, "expecting autoinc flag to be false")

	column, err = db.Column("main", "test", "id")
	checkNoError(t, err, "error getting column metadata: %s")
}

func TestForeignKeys(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	err := db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY NOT NULL);" +
		"CREATE TABLE child (id INTEGER PRIMARY KEY NOT NULL, parentId INTEGER, " +
		"FOREIGN KEY (parentId) REFERENCES parent(id));")
	checkNoError(t, err, "error creating tables: %s")
	fks, err := db.ForeignKeys("", "child")
	checkNoError(t, err, "error listing FKs: %s")
	if len(fks) != 1 {
		t.Fatalf("expected 1 FK <> %d", len(fks))
	}
	fk := fks[0]
	if fk.From[0] != "parentId" || fk.Table != "parent" || fk.To[0] != "id" {
		t.Errorf("unexpected FK data: %#v", fk)
	}
}

func TestIndexes(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	createTable(db, t)
	createIndex(db, t)

	indexes, err := db.Indexes("", "test")
	checkNoError(t, err, "error listing indexes: %s")
	if len(indexes) != 1 {
		t.Fatalf("Expected one index <> %d", len(indexes))
	}
	index := indexes[0]
	assert.Equal(t, "test_index", index.Name, "index name")
	assert.T(t, !index.Unique, "expected index 'test_index' to be not unique")

	columns, err := db.IndexColumns("", "test_index")
	checkNoError(t, err, "error listing index columns: %s")
	if len(columns) != 1 {
		t.Fatalf("expected one column <> %d", len(columns))
	}
	column := columns[0]
	assert.Equal(t, "a_string", column.Name, "column name")
}

func TestColumnMetadata(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	s, err := db.Prepare("SELECT name AS table_name FROM sqlite_master")
	check(err)
	defer checkFinalize(s, t)

	databaseName := s.ColumnDatabaseName(0)
	assert.Equal(t, "main", databaseName, "database name")
	tableName := s.ColumnTableName(0)
	assert.Equal(t, "sqlite_master", tableName, "table name")
	originName := s.ColumnOriginName(0)
	assert.Equal(t, "name", originName, "origin name")
	declType := s.ColumnDeclaredType(0)
	assert.Equal(t, "text", declType, "declared type")
	affinity := s.ColumnTypeAffinity(0)
	assert.Equal(t, Textual, affinity, "affinity")
}
