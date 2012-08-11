// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"testing"
)

func createIndex(db *Conn, t *testing.T) {
	err := db.Exec("DROP INDEX IF EXISTS test_index;" +
		"CREATE INDEX test_index on test(a_string)")
	checkNoError(t, err, "error creating index: %s")
}

func TestDatabases(t *testing.T) {
	db := open(t)
	defer db.Close()

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
	defer db.Close()

	tables, err := db.Tables("")
	checkNoError(t, err, "error looking for tables: %s")
	assertEquals(t, "expected %d table but got %d", 0, len(tables))
	createTable(db, t)
	tables, err = db.Tables("main")
	checkNoError(t, err, "error looking for tables: %s")
	assertEquals(t, "expected %d table but got %d", 1, len(tables))
	assertEquals(t, "wrong table name: %q <> %q", "test", tables[0])
}

func TestColumns(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)

	columns, err := db.Columns("", "test")
	checkNoError(t, err, "error listing columns: %s")
	if len(columns) != 4 {
		t.Fatalf("Expected 4 columns <> %d", len(columns))
	}
	column := columns[2]
	assertEquals(t, "wrong column name: %q <> %q", "int_num", column.Name)
}

func TestColumn(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)

	column, err := db.Column("", "test", "id")
	checkNoError(t, err, "error getting column metadata: %s")
	assertEquals(t, "wrong column name: %q <> %q", "id", column.Name)
	assert(t, "expecting primary key flag to be true", column.Pk)
	assert(t, "expecting autoinc flag to be false", !column.Autoinc)
}

func TestForeignKeys(t *testing.T) {
	db := open(t)
	defer db.Close()

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
	defer db.Close()
	createTable(db, t)
	createIndex(db, t)

	indexes, err := db.Indexes("", "test")
	checkNoError(t, err, "error listing indexes: %s")
	if len(indexes) != 1 {
		t.Fatalf("Expected one index <> %d", len(indexes))
	}
	index := indexes[0]
	assertEquals(t, "wrong index name: %q <> %q", "test_index", index.Name)
	assert(t, "index 'test_index' is not unique", !index.Unique)

	columns, err := db.IndexColumns("", "test_index")
	checkNoError(t, err, "error listing index columns: %s")
	if len(columns) != 1 {
		t.Fatalf("expected one column <> %d", len(columns))
	}
	column := columns[0]
	assertEquals(t, "Wrong column name: %q <> %q", "a_string", column.Name)
}

func TestColumnMetadata(t *testing.T) {
	db := open(t)
	defer db.Close()
	s, err := db.Prepare("SELECT name AS table_name FROM sqlite_master")
	check(err)
	defer s.Finalize()

	databaseName := s.ColumnDatabaseName(0)
	assertEquals(t, "wrong database name: %q <> %q", "main", databaseName)
	tableName := s.ColumnTableName(0)
	assertEquals(t, "wrong table name: %q <> %q", "sqlite_master", tableName)
	originName := s.ColumnOriginName(0)
	assertEquals(t, "wrong origin name: %q <> %q", "name", originName)
	declType := s.ColumnDeclaredType(0)
	assertEquals(t, "wrong declared type: %q <> %q", "text", declType)
}
