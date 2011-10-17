package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"testing"
)

func createIndex(db *Conn, t *testing.T) {
	err := db.Exec("DROP INDEX IF EXISTS test_index;" +
		"CREATE INDEX test_index on test(a_string)")
	if err != nil {
		t.Fatalf("error creating index: %s", err)
	}
}

func TestDatabases(t *testing.T) {
	db := open(t)
	defer db.Close()

	databases, err := db.Databases()
	if err != nil {
		t.Fatalf("error looking for databases: %s", err)
	}
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

	tables, err := db.Tables()
	if err != nil {
		t.Fatalf("error looking for tables: %s", err)
	}
	if len(tables) != 0 {
		t.Errorf("Expected no table but got %d\n", len(tables))
	}
	createTable(db, t)
	tables, err = db.Tables()
	if err != nil {
		t.Fatalf("error looking for tables: %s", err)
	}
	if len(tables) != 1 {
		t.Errorf("Expected one table but got %d\n", len(tables))
	}
	if tables[0] != "test" {
		t.Errorf("Wrong table name: 'test' <> %s\n", tables[0])
	}
}

func TestColumns(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)

	columns, err := db.Columns("test")
	if err != nil {
		t.Fatalf("error listing columns: %s", err)
	}
	if len(columns) != 4 {
		t.Fatalf("Expected 4 columns <> %d", len(columns))
	}
	column := columns[2]
	if column.Name != "int_num" {
		t.Errorf("Wrong column name: 'int_num' <> %s", column.Name)
	}
}

func TestForeignKeys(t *testing.T) {
	db := open(t)
	defer db.Close()

	err := db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY);" +
		"CREATE TABLE child (id INTEGER PRIMARY KEY, parentId INTEGER, " +
		"FOREIGN KEY (parentId) REFERENCES parent(id));")
	if err != nil {
		t.Fatalf("error creating tables: %s", err)
	}
	fks, err := db.ForeignKeys("child")
	if err != nil {
		t.Fatalf("error listing FKs: %s", err)
	}
	if len(fks) != 1 {
		t.Fatalf("Expected 1 FK <> %d", len(fks))
	}
	fk := fks[0]
	if fk.From[0] != "parentId" || fk.Table != "parent" || fk.To[0] != "id" {
		t.Errorf("Unexpected FK data: %#v", fk)
	}
}

func TestIndexes(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)
	createIndex(db, t)

	indexes, err := db.Indexes("test")
	if err != nil {
		t.Fatalf("error listing indexes: %s", err)
	}
	if len(indexes) != 1 {
		t.Fatalf("Expected one index <> %d", len(indexes))
	}
	index := indexes[0]
	if index.Name != "test_index" {
		t.Errorf("Wrong index name: 'test_index' <> %s", index.Name)
	}
	if index.Unique {
		t.Errorf("Index 'test_index' is not unique")
	}

	columns, err := db.IndexColumns("test_index")
	if err != nil {
		t.Fatalf("error listing index columns: %s", err)
	}
	if len(columns) != 1 {
		t.Fatalf("Expected one column <> %d", len(columns))
	}
	column := columns[0]
	if column.Name != "a_string" {
		t.Errorf("Wrong column name: 'a_string' <> %s", column.Name)
	}
}
