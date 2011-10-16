package sqlite_test

import (
	"fmt"
	"github.com/gwenn/gosqlite"
)

// 0
func ExampleOpen() {
	db, err := sqlite.Open(":memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	fmt.Printf("%d\n", db.TotalChanges())
}

// <nil>
func ExampleExec() {
	db, _ := sqlite.Open(":memory:")
	defer db.Close()
	err := db.Exec("CREATE TABLE test(id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
	fmt.Println(err)
}

// true <nil>
func ExamplePrepare() {
	db, _ := sqlite.Open(":memory:")
	defer db.Close()
	stmt, err := db.Prepare("SELECT 1 where 1 = ?", 1)
	if err != nil {
		panic(err)
	}
	defer stmt.Finalize()
	fmt.Println(stmt.Next())
}

// OK
func ExampleNext() {
	db, _ := sqlite.Open(":memory:")
	defer db.Close()
	stmt, err := db.Prepare("SELECT 1, 'test'")
	defer stmt.Finalize()
	var ok bool
	for ok, err = stmt.Next(); ok; ok, err = stmt.Next() {
		fmt.Println("OK")
	}
	if err != nil {
		panic(err)
	}
}

// 1 test
func ExampleScan() {
	db, _ := sqlite.Open(":memory:")
	defer db.Close()
	stmt, _ := db.Prepare("SELECT 1, 'test'")
	defer stmt.Finalize()
	var id int
	var name string
	for sqlite.Must(stmt.Next()) {
		stmt.Scan(&id, &name)
		fmt.Println(id, name)
	}
}

// 1 test
func ExampleNamedScan() {
	db, _ := sqlite.Open(":memory:")
	defer db.Close()
	stmt, _ := db.Prepare("SELECT 1 as id, 'test' as name")
	defer stmt.Finalize()
	var id int
	var name string
	for sqlite.Must(stmt.Next()) {
		stmt.NamedScan("name", &name, "id", &id)
		fmt.Println(id, name)
	}
}
