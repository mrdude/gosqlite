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
	Id    int
	Seq   int
	Table string
	From  string
	To    string
}

func (c *Conn) ForeignKeys(table string) ([]ForeignKey, os.Error) {
	s, err := c.Prepare(Mprintf("pragma foreign_key_list(%Q)", table))
	if err != nil {
		return nil, err
	}
	var fks []ForeignKey = make([]ForeignKey, 0, 20)
	var ok bool
	for ok, err = s.Next(); ok; ok, err = s.Next() {
		fk := ForeignKey{}
		err = s.NamedScan("id", &fk.Id, "seq", &fk.Seq, "table", &fk.Table, "from", &fk.From, "to", &fk.To)
		if err != nil {
			return nil, err
		}
		fks = append(fks, fk)
	}
	if err != nil {
		return nil, err
	}
	return fks, nil
}

func Mprintf(format string, arg string) string {
	cf := C.CString(format)
	defer C.free(unsafe.Pointer(cf))
	ca := C.CString(arg)
	defer C.free(unsafe.Pointer(ca))
	zSQL := C.my_mprintf(cf, ca)
	defer C.sqlite3_free(unsafe.Pointer(zSQL))
	return C.GoString(zSQL)
}
