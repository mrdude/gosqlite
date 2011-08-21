// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqlite provides access to the SQLite library, version 3.
package sqlite

/*
#include <sqlite3.h>
#include <stdlib.h>
*/
import "C"

import (
	"os"
	"unsafe"
)

type BlobReader struct {
	c  *Conn
	bl *C.sqlite3_blob
}

type BlobReadWriter struct {
	BlobReader
}

type ZeroBlobLength int

func (c *Conn) NewBlobReader(db, table, column string, row int64) (*BlobReader, os.Error) {
	bl, err := c.blob_open(db, table, column, row, false)
	if err != nil {
		return nil, err
	}
	return &BlobReader{c, bl}, nil
}

func (c *Conn) NewBlobReadWriter(db, table, column string, row int64) (*BlobReadWriter, os.Error) {
	bl, err := c.blob_open(db, table, column, row, true)
	if err != nil {
		return nil, err
	}
	return &BlobReadWriter{BlobReader{c, bl}}, nil
}

func (c *Conn) blob_open(db, table, column string, row int64, write bool) (*C.sqlite3_blob, os.Error) {
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
		return nil, os.NewError("sqlite succeeded without returning a blob")
	}
	return bl, nil
}

func (r *BlobReader) Close() os.Error {
	rv := C.sqlite3_blob_close(r.bl)
	if rv != C.SQLITE_OK {
		return r.c.error(rv)
	}
	r.bl = nil
	return nil
}

func (r *BlobReader) Read(v []byte) (int, os.Error) {
	var p *byte
	if len(v) > 0 {
		p = &v[0]
	}
	rv := C.sqlite3_blob_read(r.bl, unsafe.Pointer(p), C.int(len(v)), 0) // TODO offset
	if rv != C.SQLITE_OK {
		return 0, r.c.error(rv)
	}
	return len(v), nil
}

func (r *BlobReader) Size() (int, os.Error) {
	s := C.sqlite3_blob_bytes(r.bl)
	return int(s), nil
}

func (w *BlobReadWriter) Write(v []byte) (int, os.Error) {
	var p *byte
	if len(v) > 0 {
		p = &v[0]
	}
	rv := C.sqlite3_blob_write(w.bl, unsafe.Pointer(p), C.int(len(v)), 0) // TODO offset
	if rv != C.SQLITE_OK {
		return 0, w.c.error(rv)
	}
	return len(v), nil
}

func (r *BlobReader) Reopen(rowid int64) os.Error {
	rv := C.sqlite3_blob_reopen(r.bl, C.sqlite3_int64(rowid))
	if rv != C.SQLITE_OK {
		return r.c.error(rv)
	}
	return nil
}
