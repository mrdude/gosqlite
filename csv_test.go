// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build all

package sqlite_test

import (
	"fmt"
	"os"
	"testing"
	. "github.com/gwenn/gosqlite"
)

func TestCsvModule(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	err := LoadCsvModule(db)
	checkNoError(t, err, "couldn't create CSV module: %s")
	err = db.Exec("CREATE VIRTUAL TABLE vtab USING csv('test.csv', USE_HEADER_ROW)")
	checkNoError(t, err, "couldn't create CSV virtual table: %s")

	s, err := db.Prepare("SELECT rowid, * FROM vtab ORDER BY rowid LIMIT 3 OFFSET 2")
	checkNoError(t, err, "couldn't select from CSV virtual table: %s")
	defer checkFinalize(s, t)

	w, err := os.Open(os.DevNull)
	checkNoError(t, err, "couldn't open /dev/null: %s")
	var i int
	var col1, col2, col3 string
	err = s.Select(func(s *Stmt) (err error) {
		if err = s.Scan(&i, &col1, &col2, &col3); err != nil {
			return
		}
		fmt.Fprintf(w, "%d: %s|%s|%s\n", i, col1, col2, col3)
		return
	})
	checkNoError(t, err, "couldn't select from CSV virtual table: %s")

	err = db.Exec("DROP TABLE vtab")
	checkNoError(t, err, "couldn't drop CSV virtual table: %s")
}
