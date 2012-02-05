// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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

import "unsafe"

// Executes pragma 'database_list'
func (c *Conn) Databases() (map[string]string, error) {
	s, err := c.Prepare("PRAGMA database_list")
	if err != nil {
		return nil, err
	}
	defer s.Finalize()
	var databases map[string]string = make(map[string]string)
	var name, file string
	err = s.Select(func(s *Stmt) (err error) {
		if err = s.Scan(nil, &name, &file); err != nil {
			return
		}
		databases[name] = file
		return
	})
	if err != nil {
		return nil, err
	}
	return databases, nil
}

// Selects tables (no view) from 'sqlite_master' and filters system tables out.
// TODO Make possible to specified the database name (main.sqlite_master)
func (c *Conn) Tables() ([]string, error) {
	s, err := c.Prepare("SELECT name FROM sqlite_master WHERE type IN ('table') AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		return nil, err
	}
	defer s.Finalize()
	var tables []string = make([]string, 0, 20)
	err = s.Select(func(s *Stmt) (err error) {
		name, _ := s.ScanText(0)
		tables = append(tables, name)
		return
	})
	if err != nil {
		return nil, err
	}
	return tables, nil
}

// See Conn.Columns/IndexColumns
type Column struct {
	Cid       int
	Name      string
	DataType  string
	NotNull   bool
	DfltValue string // FIXME type ?
	Pk        bool
}

// Executes pragma 'table_info'
// TODO Make possible to specify the database-name (PRAGMA %Q.table_info(%Q))
// TODO sqlite3_table_column_metadata?
func (c *Conn) Columns(table string) ([]Column, error) {
	s, err := c.Prepare(Mprintf("PRAGMA table_info(%Q)", table))
	if err != nil {
		return nil, err
	}
	defer s.Finalize()
	var columns []Column = make([]Column, 0, 20)
	err = s.Select(func(s *Stmt) (err error) {
		c := Column{}
		if err = s.Scan(&c.Cid, &c.Name, &c.DataType, &c.NotNull, &c.DfltValue, &c.Pk); err != nil {
			return
		}
		columns = append(columns, c)
		return
	})
	if err != nil {
		return nil, err
	}
	return columns, nil
}

// See Conn.ForeignKeys
type ForeignKey struct {
	Table string
	From  []string
	To    []string
}

// Executes pragma 'foreign_key_list'
// TODO How to specify a database-name?
func (c *Conn) ForeignKeys(table string) (map[int]*ForeignKey, error) {
	s, err := c.Prepare(Mprintf("PRAGMA foreign_key_list(%Q)", table))
	if err != nil {
		return nil, err
	}
	defer s.Finalize()
	var fks = make(map[int]*ForeignKey)
	var id, seq int
	var ref, from, to string
	err = s.Select(func(s *Stmt) (err error) {
		if err = s.NamedScan("id", &id, "seq", &seq, "table", &ref, "from", &from, "to", &to); err != nil {
			return
		}
		fk, ex := fks[id]
		if !ex {
			fk = &ForeignKey{Table: ref}
			fks[id] = fk
		}
		// TODO Ensure columns are appended in the correct order...
		fk.From = append(fk.From, from)
		fk.To = append(fk.To, to)
		return
	})
	if err != nil {
		return nil, err
	}
	return fks, nil
}

// See Conn.Indexes
type Index struct {
	Name   string
	Unique bool
}

// Executes pragma 'index_list'
// TODO How to specify a database-name?
func (c *Conn) Indexes(table string) ([]Index, error) {
	s, err := c.Prepare(Mprintf("PRAGMA index_list(%Q)", table))
	if err != nil {
		return nil, err
	}
	defer s.Finalize()
	var indexes []Index = make([]Index, 0, 5)
	err = s.Select(func(s *Stmt) (err error) {
		i := Index{}
		if err = s.Scan(nil, &i.Name, &i.Unique); err != nil {
			return
		}
		indexes = append(indexes, i)
		return
	})
	if err != nil {
		return nil, err
	}
	return indexes, nil
}

// Executes pragma 'index_info'
// Only Column.Cid and Column.Name are specified. All other fields are unspecifed.
func (c *Conn) IndexColumns(index string) ([]Column, error) {
	s, err := c.Prepare(Mprintf("PRAGMA index_info(%Q)", index))
	if err != nil {
		return nil, err
	}
	defer s.Finalize()
	var columns []Column = make([]Column, 0, 5)
	err = s.Select(func(s *Stmt) (err error) {
		c := Column{}
		if err = s.Scan(nil, &c.Cid, &c.Name); err != nil {
			return
		}
		columns = append(columns, c)
		return
	})
	if err != nil {
		return nil, err
	}
	return columns, nil
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
