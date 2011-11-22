package sqlite_test

import (
	"exp/sql"
	"testing"
)

const (
	ddl = "DROP TABLE IF EXISTS test;" +
		"CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT," +
		" name TEXT);"
	dml = "INSERT INTO test (name) values ('Bart');" +
		"INSERT INTO test (name) values ('Lisa');" +
		"UPDATE test set name = 'El Barto' where name = 'Bart';" +
		"DELETE from test where name = 'Bart';"
	insert = "INSERT into test (name) values (?)"
	query  = "SELECT * FROM test where name like ?"
)

func sqlOpen(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", "")
	if err != nil {
		t.Fatalf("Error opening database: %s", err)
	}
	return db
}

func sqlCreate(ddl string, t *testing.T) *sql.DB {
	db := sqlOpen(t)
	_, err := db.Exec(ddl)
	if err != nil {
		t.Fatalf("Error creating table: %s", err)
	}
	return db
}

func TestSqlOpen(t *testing.T) {
	db := sqlOpen(t)
	if err := db.Close(); err != nil {
		t.Fatalf("Error closing database: %s", err)
	}
}

func TestSqlDdl(t *testing.T) {
	db := sqlOpen(t)
	defer db.Close()
	result, err := db.Exec(ddl)
	if err != nil {
		t.Fatalf("Error creating table: %s", err)
	}
	_, err = result.LastInsertId() // FIXME Error expected
	if err == nil {
		t.Logf("Error expected when calling LastInsertId after DDL")
	}
	_, err = result.RowsAffected() // FIXME Error expected
	if err == nil {
		t.Logf("Error expected when calling RowsAffected after DDL")
	}
}

func TestSqlDml(t *testing.T) {
	db := sqlCreate(ddl, t)
	defer db.Close()
	result, err := db.Exec(dml)
	if err != nil {
		t.Fatalf("Error updating data: %s", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Errorf("Error while calling LastInsertId: %s", err)
	}
	if id != 2 {
		t.Errorf("Expected %d got %d LastInsertId", 2, id)
	}
	changes, err := result.RowsAffected()
	if err != nil {
		t.Errorf("Error while calling RowsAffected: %s", err)
	}
	if changes != 0 {
		t.Errorf("Expected %d got %d RowsAffected", 0, changes)
	}
}

func TestSqlInsert(t *testing.T) {
	db := sqlCreate(ddl, t)
	defer db.Close()
	result, err := db.Exec(insert, "Bart")
	if err != nil {
		t.Fatalf("Error updating data: %s", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Errorf("Error while calling LastInsertId: %s", err)
	}
	if id != 1 {
		t.Errorf("Expected %d got %d LastInsertId", 2, id)
	}
	changes, err := result.RowsAffected()
	if err != nil {
		t.Errorf("Error while calling RowsAffected: %s", err)
	}
	if changes != 1 {
		t.Errorf("Expected %d got %d RowsAffected", 0, changes)
	}
}

func TestSqlExecWithIllegalCmd(t *testing.T) {
	db := sqlCreate(ddl+dml, t)
	defer db.Close()

	_, err := db.Exec(query, "%")
	if err == nil {
		t.Fatalf("Error expected when calling Exec with a SELECT")
	}
}

func TestSqlQuery(t *testing.T) {
	db := sqlCreate(ddl+dml, t)
	defer db.Close()

	rows, err := db.Query(query, "%")
	defer rows.Close()
	var id int
	var name string
	for rows.Next() {
		err = rows.Scan(&id, &name)
		if err != nil {
			t.Errorf("Error while scanning: %s", err)
		}
	}
	// FIXME Dangling statement
}

func TestSqlTx(t *testing.T) {
	db := sqlCreate(ddl, t)
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Errorf("Error while begining tx: %s", err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Errorf("Error while rollbacking tx: %s", err)
	}
}

func TestSqlPrepare(t *testing.T) {
	db := sqlCreate(ddl+dml, t)
	defer db.Close()

	stmt, err := db.Prepare(insert)
	if err != nil {
		t.Errorf("Error while preparing stmt: %s", err)
	}
	defer stmt.Close()
	_, err = stmt.Exec("Bart")
	if err != nil {
		t.Errorf("Error while executing stmt: %s", err)
	}
}
