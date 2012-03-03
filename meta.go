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

// just to get ride of warning
static int my_table_column_metadata(
  sqlite3 *db,
  const char *zDbName,
  const char *zTableName,
  const char *zColumnName,
  char **pzDataType,
  char **pzCollSeq,
  int *pNotNull,
  int *pPrimaryKey,
  int *pAutoinc
) {
	return sqlite3_table_column_metadata(db, zDbName, zTableName, zColumnName,
		(char const **)pzDataType, (char const **)pzCollSeq, pNotNull, pPrimaryKey, pAutoinc);
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
	defer s.finalize()
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
	defer s.finalize()
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
	Autoinc   bool
	CollSeq   string
}

// Executes pragma 'table_info'
// TODO Make possible to specify the database-name (PRAGMA %Q.table_info(%Q))
func (c *Conn) Columns(table string) ([]Column, error) {
	s, err := c.Prepare(Mprintf("PRAGMA table_info(%Q)", table))
	if err != nil {
		return nil, err
	}
	defer s.finalize()
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

// Extract metadata about a column of a table
// (See http://sqlite.org/c3ref/table_column_metadata.html)
func (c *Conn) Column(dbName, tableName, columnName string) (*Column, error) {
	var zDbName *C.char
	if len(dbName) > 0 {
		zDbName = C.CString(dbName)
		defer C.free(unsafe.Pointer(zDbName))
	}
	zTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(zTableName))
	zColumnName := C.CString(columnName)
	defer C.free(unsafe.Pointer(zColumnName))
	var zDataType, zCollSeq *C.char
	var notNull, primaryKey, autoinc C.int
	rv := C.my_table_column_metadata(c.db, zDbName, zTableName, zColumnName, &zDataType, &zCollSeq,
		&notNull, &primaryKey, &autoinc)
	if rv != C.SQLITE_OK {
		return nil, c.error(rv)
	}
	return &Column{-1, columnName, C.GoString(zDataType), notNull == 1, "", primaryKey == 1,
		autoinc == 1, C.GoString(zCollSeq)}, nil
}

// See Conn.ForeignKeys
type ForeignKey struct {
	Table string
	From  []string
	To    []string
}

// Executes pragma 'foreign_key_list'
// TODO Make possible to specify the database-name (PRAGMA %Q.foreign_key_list(%Q))
func (c *Conn) ForeignKeys(table string) (map[int]*ForeignKey, error) {
	s, err := c.Prepare(Mprintf("PRAGMA foreign_key_list(%Q)", table))
	if err != nil {
		return nil, err
	}
	defer s.finalize()
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
// TODO Make possible to specify the database-name (PRAGMA %Q.index_list(%Q))
func (c *Conn) Indexes(table string) ([]Index, error) {
	s, err := c.Prepare(Mprintf("PRAGMA index_list(%Q)", table))
	if err != nil {
		return nil, err
	}
	defer s.finalize()
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
	defer s.finalize()
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

// (See http://sqlite.org/c3ref/mprintf.html)
func Mprintf(format string, arg string) string {
	cf := C.CString(format)
	defer C.free(unsafe.Pointer(cf))
	ca := C.CString(arg)
	defer C.free(unsafe.Pointer(ca))
	zSQL := C.my_mprintf(cf, ca)
	defer C.sqlite3_free(unsafe.Pointer(zSQL))
	return C.GoString(zSQL)
}
