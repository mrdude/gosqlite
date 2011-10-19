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

/*
import (
	"fmt"
	"os"
	"unsafe"
)
*/

// Calls sqlite3_column_count and sqlite3_column_(blob|double|int|int64|text) depending on columns type.
// http://sqlite.org/c3ref/column_blob.html
/*
func (s *Stmt) ScanNamedValues(values ...NamedValue) os.Error {
	n := s.ColumnCount()
	if n != len(values) { // What happens when the number of arguments is less than the number of columns?
		return os.NewError(fmt.Sprintf("incorrect argument count for Stmt.ScanValues: have %d want %d", len(values), n))
	}

	for _, v := range values {
		index, err := s.ColumnIndex(v.Name()) // How to look up only once for one statement ?
		if err != nil {
			return err
		}
		s.ScanValue(index, v)
	}
	return nil
}
*/

// Calls sqlite3_column_count and sqlite3_column_(blob|double|int|int64|text) depending on columns type.
// http://sqlite.org/c3ref/column_blob.html
/*
func (s *Stmt) ScanValues(values ...Value) os.Error {
	n := s.ColumnCount()
	if n != len(values) { // What happens when the number of arguments is less than the number of columns?
		return os.NewError(fmt.Sprintf("incorrect argument count for Stmt.ScanValues: have %d want %d", len(values), n))
	}

	for i, v := range values {
		s.ScanValue(i, v)
	}
	return nil
}
*/

// The leftmost column/index is number 0.
// Calls sqlite3_column_(blob|double|int|int64|text) depending on columns type.
// http://sqlite.org/c3ref/column_blob.html
/*
func (s *Stmt) ScanValue(index int) {
	switch s.columnType(index) {
	case C.SQLITE_NULL:
		value.setNull(true)
	case C.SQLITE_TEXT:
		p := C.sqlite3_column_text(s.stmt, C.int(index))
		n := C.sqlite3_column_bytes(s.stmt, C.int(index))
		value.setText(C.GoStringN((*C.char)(unsafe.Pointer(p)), n))
	case C.SQLITE_INTEGER:
		value.setInt(int64(C.sqlite3_column_int64(s.stmt, C.int(index))))
	case C.SQLITE_FLOAT:
		value.setFloat(float64(C.sqlite3_column_double(s.stmt, C.int(index))))
	case C.SQLITE_BLOB:
		p := C.sqlite3_column_blob(s.stmt, C.int(index))
		n := C.sqlite3_column_bytes(s.stmt, C.int(index))
		value.setBlob((*[1 << 30]byte)(unsafe.Pointer(p))[0:n])
	default:
		panic("The column type is not one of SQLITE_INTEGER, SQLITE_FLOAT, SQLITE_TEXT, SQLITE_BLOB, or SQLITE_NULL")
	}
}
*/
