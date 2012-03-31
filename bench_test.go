// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"testing"
)

func fill(db *Conn, n int) {
	db.Exec("DROP TABLE IF EXISTS test")
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, float_num REAL, int_num INTEGER, a_string TEXT)")
	s, _ := db.Prepare("INSERT INTO test (float_num, int_num, a_string) VALUES (?, ?, ?)")

	db.Begin()
	for i := 0; i < n; i++ {
		s.Exec(float64(i)*float64(3.14), i, "hello")
	}
	s.Finalize()
	db.Commit()
}

func BenchmarkValuesScan(b *testing.B) {
	b.StopTimer()
	db, _ := Open("")
	defer db.Close()
	fill(db, 1)

	cs, _ := db.Prepare("SELECT float_num, int_num, a_string FROM test")
	defer cs.Finalize()

	b.StartTimer()
	for i := 0; i < b.N; i++ {

		values := make([]interface{}, 3)
		if Must(cs.Next()) {
			cs.ScanValues(values)
		}
		cs.Reset()
	}
}

func BenchmarkScan(b *testing.B) {
	b.StopTimer()
	db, _ := Open("")
	defer db.Close()
	fill(db, 1)

	cs, _ := db.Prepare("SELECT float_num, int_num, a_string FROM test")
	defer cs.Finalize()

	b.StartTimer()
	for i := 0; i < b.N; i++ {

		var fnum float64
		var inum int64
		var sstr string

		if Must(cs.Next()) {
			cs.Scan(&fnum, &inum, &sstr)
		}
		cs.Reset()
	}
}

func BenchmarkNamedScan(b *testing.B) {
	b.StopTimer()
	db, _ := Open("")
	defer db.Close()
	fill(db, 1)

	cs, _ := db.Prepare("SELECT float_num, int_num, a_string FROM test")
	defer cs.Finalize()

	b.StartTimer()
	for i := 0; i < b.N; i++ {

		var fnum float64
		var inum int64
		var sstr string

		if Must(cs.Next()) {
			cs.NamedScan("float_num", &fnum, "int_num", &inum, "a_string", &sstr)
		}
		cs.Reset()
	}
}

func BenchmarkInsert(b *testing.B) {
	db, _ := Open("")
	defer db.Close()
	fill(db, b.N)
}

func BenchmarkNamedInsert(b *testing.B) {
	db, _ := Open("")
	defer db.Close()
	db.Exec("DROP TABLE IF EXISTS test")
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL," +
		" float_num REAL, int_num INTEGER, a_string TEXT)")
	s, _ := db.Prepare("INSERT INTO test (float_num, int_num, a_string)" +
		" VALUES (:f, :i, :s)")
	defer s.Finalize()

	db.Begin()
	for i := 0; i < b.N; i++ {
		s.NamedBind("f", float64(i)*float64(3.14), "i", i, "s", "hello")
		s.Next()
	}
	db.Commit()
}
