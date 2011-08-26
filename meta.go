// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqlite provides access to the SQLite library, version 3.

package sqlite

/*
#include <sqlite3.h>
#include <stdlib.h>

// cgo doesn't support varargs
static char *my_mprintf(char *zFormat, char *arg) {
	return sqlite3_mprintf(zFormat, arg);
}
*/
import "C"

import (
	"os"
	"unsafe"
)

// Selects tables (no view) from 'sqlite_master' and filters system tables out.
func (c *Conn) Tables() ([]string, os.Error) {
	s, err := c.Prepare("SELECT name FROM sqlite_master WHERE type IN ('table') AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		return nil, err
	}
	var tables []string = make([]string, 0, 20)
	var ok bool
	var name string
	for ok, err = s.Next(); ok; ok, err = s.Next() {
		s.Scan(&name)
		tables = append(tables, name)
	}
	if err != nil {
		return nil, err
	}
	return tables, nil
}

type Column struct {
	Cid       int
	Name      string
	DataType  string
	NotNull   bool
	DfltValue string // FIXME type ?
	Pk        bool
}

// Executes pragma 'table_info'
func (c *Conn) Columns(table string) ([]Column, os.Error) {
	s, err := c.Prepare(Mprintf("pragma table_info(%Q)", table))
	if err != nil {
		return nil, err
	}
	var columns []Column = make([]Column, 0, 20)
	var ok bool
	for ok, err = s.Next(); ok; ok, err = s.Next() {
		c := Column{}
		err = s.Scan(&c.Cid, &c.Name, &c.DataType, &c.NotNull, &c.DfltValue, &c.Pk)
		if err != nil {
			return nil, err
		}
		columns = append(columns, c)
	}
	if err != nil {
		return nil, err
	}
	return columns, nil
}

type ForeignKey struct {
	Table string
	From  []string
	To    []string
}

// Executes pragma 'foreign_key_list'
func (c *Conn) ForeignKeys(table string) (map[int]*ForeignKey, os.Error) {
	s, err := c.Prepare(Mprintf("pragma foreign_key_list(%Q)", table))
	if err != nil {
		return nil, err
	}
	var fks = make(map[int]*ForeignKey)
	var ok bool
	var id, seq int
	var ref, from, to string
	for ok, err = s.Next(); ok; ok, err = s.Next() {
		err = s.NamedScan("id", &id, "seq", &seq, "table", &ref, "from", &from, "to", &to)
		if err != nil {
			return nil, err
		}
		fk, ex := fks[id]
		if !ex {
			fk = &ForeignKey{Table: ref}
			fks[id] = fk
		}
		// TODO Ensure columns are appended in the correct order...
		fk.From = append(fk.From, from)
		fk.To = append(fk.To, to)
	}
	if err != nil {
		return nil, err
	}
	return fks, nil
}

// Calls http://sqlite.org/c3ref/mprintf.html
func Mprintf(format string, arg string) string {
	cf := C.CString(format)
	defer C.free(unsafe.Pointer(cf))
	ca := C.CString(arg)
	defer C.free(unsafe.Pointer(ca))
	zSQL := C.my_mprintf(cf, ca)
	defer C.sqlite3_free(unsafe.Pointer(zSQL))
	return C.GoString(zSQL)
}
