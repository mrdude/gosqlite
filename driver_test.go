// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	"database/sql"
	"testing"

	"github.com/bmizerany/assert"
	"github.com/gwenn/gosqlite"
)

const (
	ddl = "DROP TABLE IF EXISTS test;" +
		"CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL," +
		" name TEXT);"
	dml = "INSERT INTO test (name) VALUES ('Bart');" +
		"INSERT INTO test (name) VALUES ('Lisa');" +
		"UPDATE test SET name = 'El Barto' WHERE name = 'Bart';" +
		"DELETE FROM test WHERE name = 'Bart';"
	insert = "INSERT INTO test (name) VALUES (?)"
	query  = "SELECT * FROM test WHERE name LIKE ?"
)

func sqlOpen(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	checkNoError(t, err, "Error opening database: %s")
	return db
}

func checkSqlDbClose(db *sql.DB, t *testing.T) {
	checkNoError(t, db.Close(), "Error closing connection: %s")
}

func checkSqlStmtClose(stmt *sql.Stmt, t *testing.T) {
	checkNoError(t, stmt.Close(), "Error closing statement: %s")
}

func checkSqlRowsClose(rows *sql.Rows, t *testing.T) {
	checkNoError(t, rows.Close(), "Error closing rows: %s")
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
	defer checkSqlDbClose(db, t)
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
	defer checkSqlDbClose(db, t)
	result, err := db.Exec(dml)
	checkNoError(t, err, "Error updating data: %s")
	id, err := result.LastInsertId()
	checkNoError(t, err, "Error while calling LastInsertId: %s")
	assert.Equal(t, int64(2), id, "lastInsertId")
	changes, err := result.RowsAffected()
	checkNoError(t, err, "Error while calling RowsAffected: %s")
	assert.Equal(t, int64(0), changes, "rowsAffected")
}

func TestSqlInsert(t *testing.T) {
	db := sqlCreate(ddl, t)
	defer checkSqlDbClose(db, t)
	result, err := db.Exec(insert, "Bart")
	checkNoError(t, err, "Error updating data: %s")
	id, err := result.LastInsertId()
	checkNoError(t, err, "Error while calling LastInsertId: %s")
	assert.Equal(t, int64(1), id, "lastInsertId")
	changes, err := result.RowsAffected()
	checkNoError(t, err, "Error while calling RowsAffected: %s")
	assert.Equal(t, int64(1), changes, "rowsAffected")
}

func TestSqlExecWithIllegalCmd(t *testing.T) {
	db := sqlCreate(ddl+dml, t)
	defer checkSqlDbClose(db, t)

	_, err := db.Exec(query, "%")
	if err == nil {
		t.Fatalf("Error expected when calling Exec with a SELECT")
	}
}

func TestSqlQuery(t *testing.T) {
	db := sqlCreate(ddl+dml, t)
	defer checkSqlDbClose(db, t)

	rows, err := db.Query(query, "%")
	defer checkSqlRowsClose(rows, t)
	var id int
	var name string
	for rows.Next() {
		err = rows.Scan(&id, &name)
		checkNoError(t, err, "Error while scanning: %s")
	}
}

func TestSqlTx(t *testing.T) {
	db := sqlCreate(ddl, t)
	defer checkSqlDbClose(db, t)

	tx, err := db.Begin()
	checkNoError(t, err, "Error while begining tx: %s")
	err = tx.Rollback()
	checkNoError(t, err, "Error while rollbacking tx: %s")
}

func TestSqlPrepare(t *testing.T) {
	db := sqlCreate(ddl+dml, t)
	defer checkSqlDbClose(db, t)

	stmt, err := db.Prepare(insert)
	checkNoError(t, err, "Error while preparing stmt: %s")
	defer checkSqlStmtClose(stmt, t)
	result, err := stmt.Exec("Bart")
	checkNoError(t, err, "Error while executing stmt: %s")
	id, err := result.LastInsertId()
	checkNoError(t, err, "Error while calling LastInsertId: %s")
	assert.Equal(t, int64(3), id, "lastInsertId")
	changes, err := result.RowsAffected()
	checkNoError(t, err, "Error while calling RowsAffected: %s")
	assert.Equal(t, int64(1), changes, "rowsAffected")
}

func TestRowsWithStmtClosed(t *testing.T) {
	db := sqlCreate(ddl+dml, t)
	defer checkSqlDbClose(db, t)

	stmt, err := db.Prepare(query)
	checkNoError(t, err, "Error while preparing stmt: %s")
	//defer stmt.Close()

	rows, err := stmt.Query("%")
	checkSqlStmtClose(stmt, t)
	defer checkSqlRowsClose(rows, t)
	var id int
	var name string
	for rows.Next() {
		err = rows.Scan(&id, &name)
		checkNoError(t, err, "Error while scanning: %s")
	}
}

func TestUnwrap(t *testing.T) {
	db := sqlOpen(t)
	conn := sqlite.Unwrap(db)
	assert.Tf(t, conn != nil, "got %#v; want *sqlite.Conn", conn)
	// fmt.Printf("%#v\n", conn)
	conn.TotalChanges()
}
