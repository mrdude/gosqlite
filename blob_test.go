// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"io"
	"testing"
)

func TestBlob(t *testing.T) {
	db := open(t)
	defer db.Close()

	err := db.Exec("CREATE TABLE test (content BLOB);")
	checkNoError(t, err, "error creating table: %s")
	s, err := db.Prepare("INSERT INTO test VALUES (?)")
	checkNoError(t, err, "prepare error: %s")
	if s == nil {
		t.Fatal("statement is nil")
	}
	defer s.Finalize()
	err = s.Exec(ZeroBlobLength(10))
	checkNoError(t, err, "insert error: %s")
	rowid := db.LastInsertRowid()

	bw, err := db.NewBlobReadWriter("main", "test", "content", rowid)
	checkNoError(t, err, "blob open error: %s")
	defer bw.Close()
	content := []byte("Clob")
	n, err := bw.Write(content)
	checkNoError(t, err, "blob write error: %s")
	bw.Close()

	br, err := db.NewBlobReader("main", "test", "content", rowid)
	checkNoError(t, err, "blob open error: %s")
	defer br.Close()
	size, err := br.Size()
	checkNoError(t, err, "blob size error: %s")

	content = make([]byte, size+5)
	n, err = br.Read(content[:5])
	checkNoError(t, err, "blob read error: %s")
	assertEquals(t, "expected %d bytes but got %d", 5, n)

	n, err = br.Read(content[5:])
	checkNoError(t, err, "blob read error: %s")
	assertEquals(t, "expected %d bytes but got %d", 5, n)
	//fmt.Printf("%#v\n", content)

	n, err = br.Read(content[10:])
	assert(t, "error expected", n == 0 && err == io.EOF)
	br.Close()
}

func TestBlobMisuse(t *testing.T) {
	db := open(t)
	defer db.Close()

	bw, err := db.NewBlobReadWriter("main", "test", "content", 0)
	assert(t, "error expected", bw == nil && err != nil)
	err = bw.Close()
	assert(t, "error expected", err != nil)
}
