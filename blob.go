// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

/*
#include <sqlite3.h>
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"unsafe"
)

// Reader adapter to BLOB
type BlobReader struct {
	c          *Conn
	bl         *C.sqlite3_blob
	ReadOffset int
}

// ReadWriter adapter to BLOB
type BlobReadWriter struct {
	BlobReader
	WriteOffset int
}

// Zeroblobs are used to reserve space for a BLOB that is later written.
//
// Example:
//	s, err := db.Prepare("INSERT INTO test VALUES (?)")
//	// check err
//	defer s.Finalize()
//	err = s.Exec(ZeroBlobLength(10))
//	// check err
type ZeroBlobLength int

// Open a BLOB for incremental I/O
// Example:
//	br, err := db.NewBlobReader("db_name", "table_name", "column_name", rowid)
//  // check err
//	defer br.Close()
//	size, err := br.Size()
//  // check err
//	content = make([]byte, size)
//	n, err = br.Read(content)
//  // check err
//
// Calls http://sqlite.org/c3ref/blob_open.html
// TODO A real 'incremental' example...
func (c *Conn) NewBlobReader(db, table, column string, row int64) (*BlobReader, error) {
	bl, err := c.blob_open(db, table, column, row, false)
	if err != nil {
		return nil, err
	}
	return &BlobReader{c, bl, 0}, nil
}

// Open a BLOB For incremental I/O
// Calls http://sqlite.org/c3ref/blob_open.html
func (c *Conn) NewBlobReadWriter(db, table, column string, row int64) (*BlobReadWriter, error) {
	bl, err := c.blob_open(db, table, column, row, true)
	if err != nil {
		return nil, err
	}
	return &BlobReadWriter{BlobReader{c, bl, 0}, 0}, nil
}

func (c *Conn) blob_open(db, table, column string, row int64, write bool) (*C.sqlite3_blob, error) {
	zDb := C.CString(db)
	defer C.free(unsafe.Pointer(zDb))
	zTable := C.CString(table)
	defer C.free(unsafe.Pointer(zTable))
	zColumn := C.CString(column)
	defer C.free(unsafe.Pointer(zColumn))
	var bl *C.sqlite3_blob
	rv := C.sqlite3_blob_open(c.db, zDb, zTable, zColumn, C.sqlite3_int64(row), btocint(write), &bl)
	if rv != C.SQLITE_OK {
		if bl != nil {
			C.sqlite3_blob_close(bl)
		}
		return nil, c.error(rv)
	}
	if bl == nil {
		return nil, errors.New("sqlite succeeded without returning a blob")
	}
	return bl, nil
}

// Close a BLOB handle
// Calls http://sqlite.org/c3ref/blob_close.html
func (r *BlobReader) Close() error {
	rv := C.sqlite3_blob_close(r.bl)
	if rv != C.SQLITE_OK {
		return r.c.error(rv)
	}
	r.bl = nil
	return nil
}

// Read data from a BLOB incrementally
// Calls http://sqlite.org/c3ref/blob_read.html
func (r *BlobReader) Read(v []byte) (int, error) {
	var p *byte
	if len(v) > 0 {
		p = &v[0]
	}
	rv := C.sqlite3_blob_read(r.bl, unsafe.Pointer(p), C.int(len(v)), C.int(r.ReadOffset))
	if rv != C.SQLITE_OK {
		return 0, r.c.error(rv)
	}
	r.ReadOffset += len(v)
	return len(v), nil
}

// Return the size of an open BLOB
// Calls http://sqlite.org/c3ref/blob_bytes.html
func (r *BlobReader) Size() (int, error) {
	s := C.sqlite3_blob_bytes(r.bl)
	return int(s), nil
}

// Write data into a BLOB incrementally
// Calls http://sqlite.org/c3ref/blob_write.html
func (w *BlobReadWriter) Write(v []byte) (int, error) {
	var p *byte
	if len(v) > 0 {
		p = &v[0]
	}
	rv := C.sqlite3_blob_write(w.bl, unsafe.Pointer(p), C.int(len(v)), C.int(w.WriteOffset))
	if rv != C.SQLITE_OK {
		return 0, w.c.error(rv)
	}
	w.WriteOffset += len(v)
	return len(v), nil
}

// Move a BLOB handle to a new row
// Calls http://sqlite.org/c3ref/blob_reopen.html
func (r *BlobReader) Reopen(rowid int64) error {
	rv := C.sqlite3_blob_reopen(r.bl, C.sqlite3_int64(rowid))
	if rv != C.SQLITE_OK {
		return r.c.error(rv)
	}
	r.ReadOffset = 0
	return nil
}
