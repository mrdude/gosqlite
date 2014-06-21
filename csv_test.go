// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/bmizerany/assert"
	. "github.com/gwenn/gosqlite"
	"github.com/gwenn/yacr"
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

func TestImportCSV(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	filename := "test.csv"
	file, err := os.Open(filename)
	checkNoError(t, err, "error opening CSV file: %s")
	defer file.Close()

	ic := ImportConfig{
		Name:      filename,
		Separator: ',',
		Quoted:    true,
		Headers:   true,
		Log:       os.Stderr,
	}

	err = db.ImportCSV(file, ic, "", "test")
	checkNoError(t, err, "error while importing CSV file: %s")
}

func TestExportTableToCSV(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	createTable(db, t)
	err := db.FastExec(`INSERT INTO test (float_num, int_num, a_string) VALUES (1.23, 0, 'qu"ote'), (NULL, 1, "new
line"), (3.33, 2, 'test')`)
	checkNoError(t, err, "error while inserting data: %s")

	var b bytes.Buffer
	w := yacr.NewWriter(&b, ',', true)
	err = db.ExportTableToCSV("", "test", "", true, w)
	checkNoError(t, err, "error while exporting CSV file: %s")
	assert.Equal(t, `id,float_num,int_num,a_string
1,1.23,0,"qu""ote"
2,,1,"new
line"
3,3.33,2,test
`, b.String())
}

func TestExportToCSV(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	createTable(db, t)
	err := db.FastExec(`INSERT INTO test (float_num, int_num, a_string) VALUES (1.23, 0, 'qu"ote'), (NULL, 1, "new
line"), (3.33, 2, 'test')`)
	checkNoError(t, err, "error while inserting data: %s")

	var b bytes.Buffer
	w := yacr.NewWriter(&b, ',', true)
	s, err := db.Prepare("SELECT float_num, int_num, a_string FROM test where id > ?", 0)
	checkNoError(t, err, "error while preparing stmt: %s")
	defer checkFinalize(s, t)

	err = s.ExportToCSV("", false, w)
	checkNoError(t, err, "error while exporting CSV file: %s")
	assert.Equal(t, `1.23,0,"qu""ote"
,1,"new
line"
3.33,2,test
`, b.String())
}
