package sqlite_test

import (
	"database/sql"
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
	checkNoError(t, err, "Error opening database: %s")
	return db
}

func sqlCreate(ddl string, t *testing.T) *sql.DB {
	db := sqlOpen(t)
	_, err := db.Exec(ddl)
	checkNoError(t, err, "Error creating table: %s")
	return db
}

func TestSqlOpen(t *testing.T) {
	db := sqlOpen(t)
	checkNoError(t, db.Close(), "Error closing database: %s")
}

func TestSqlDdl(t *testing.T) {
	db := sqlOpen(t)
	defer db.Close()
	result, err := db.Exec(ddl)
	checkNoError(t, err, "Error creating table: %s")
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
	checkNoError(t, err, "Error updating data: %s")
	id, err := result.LastInsertId()
	checkNoError(t, err, "Error while calling LastInsertId: %s")
	if id != 2 {
		t.Errorf("Expected %d got %d LastInsertId", 2, id)
	}
	changes, err := result.RowsAffected()
	checkNoError(t, err, "Error while calling RowsAffected: %s")
	if changes != 0 {
		t.Errorf("Expected %d got %d RowsAffected", 0, changes)
	}
}

func TestSqlInsert(t *testing.T) {
	db := sqlCreate(ddl, t)
	defer db.Close()
	result, err := db.Exec(insert, "Bart")
	checkNoError(t, err, "Error updating data: %s")
	id, err := result.LastInsertId()
	checkNoError(t, err, "Error while calling LastInsertId: %s")
	if id != 1 {
		t.Errorf("Expected %d got %d LastInsertId", 2, id)
	}
	changes, err := result.RowsAffected()
	checkNoError(t, err, "Error while calling RowsAffected: %s")
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
		checkNoError(t, err, "Error while scanning: %s")
	}
	// FIXME Dangling statement
}

func TestSqlTx(t *testing.T) {
	db := sqlCreate(ddl, t)
	defer db.Close()

	tx, err := db.Begin()
	checkNoError(t, err, "Error while begining tx: %s")
	err = tx.Rollback()
	checkNoError(t, err, "Error while rollbacking tx: %s")
}

func TestSqlPrepare(t *testing.T) {
	db := sqlCreate(ddl+dml, t)
	defer db.Close()

	stmt, err := db.Prepare(insert)
	checkNoError(t, err, "Error while preparing stmt: %s")
	defer stmt.Close()
	_, err = stmt.Exec("Bart")
	checkNoError(t, err, "Error while executing stmt: %s")
}
