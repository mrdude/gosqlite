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
static char *my_mprintf2(char *zFormat, char *arg1, char *arg2) {
	return sqlite3_mprintf(zFormat, arg1, arg2);
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

import (
	"fmt"
	"unsafe"
)

// Databases returns one couple (name, file) for each database attached to the current database connection.
// (See http://www.sqlite.org/pragma.html#pragma_database_list)
func (c *Conn) Databases() (map[string]string, error) {
	s, err := c.prepare("PRAGMA database_list")
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

// Tables returns tables (no view) from 'sqlite_master' and filters system tables out.
// TODO create Views method to return views...
func (c *Conn) Tables(dbName string) ([]string, error) {
	var sql string
	if len(dbName) == 0 {
		sql = "SELECT name FROM sqlite_master WHERE type IN ('table') AND name NOT LIKE 'sqlite_%' ORDER BY 1"
	} else {
		sql = Mprintf("SELECT name FROM %Q.sqlite_master WHERE type IN ('table') AND name NOT LIKE 'sqlite_%%' ORDER BY 1", dbName)
	}
	s, err := c.prepare(sql)
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

// Columns returns a description for each column in the named table.
// Column.Autoinc and Column.CollSeq are left unspecified.
// (See http://www.sqlite.org/pragma.html#pragma_table_info)
func (c *Conn) Columns(dbName, table string) ([]Column, error) {
	var pragma string
	if len(dbName) == 0 {
		pragma = Mprintf("PRAGMA table_info(%Q)", table)
	} else {
		pragma = Mprintf2("PRAGMA %Q.table_info(%Q)", dbName, table)
	}
	s, err := c.prepare(pragma)
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

// Column extracts metadata about a column of a table.
// Column.Cid and Column.DfltValue are left unspecified.
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
		return nil, c.error(rv, fmt.Sprintf("Conn.Column(db: %q, tbl: %q, col: %q)", dbName, tableName, columnName))
	}
	// TODO How to avoid copy?
	return &Column{-1, columnName, C.GoString(zDataType), notNull == 1, "", primaryKey == 1,
		autoinc == 1, C.GoString(zCollSeq)}, nil
}

// ColumnDatabaseName returns the database
// that is the origin of a particular result column in SELECT statement.
// The left-most column is column 0.
// (See http://www.sqlite.org/c3ref/column_database_name.html)
func (s *Stmt) ColumnDatabaseName(index int) string {
	return C.GoString(C.sqlite3_column_database_name(s.stmt, C.int(index)))
}

// ColumnTableName returns the original un-aliased table name
// that is the origin of a particular result column in SELECT statement.
// The left-most column is column 0.
// (See http://www.sqlite.org/c3ref/column_database_name.html)
func (s *Stmt) ColumnTableName(index int) string {
	return C.GoString(C.sqlite3_column_table_name(s.stmt, C.int(index)))
}

// ColumnOriginName returns the original un-aliased table column name
// that is the origin of a particular result column in SELECT statement.
// The left-most column is column 0.
// (See http://www.sqlite.org/c3ref/column_database_name.html)
func (s *Stmt) ColumnOriginName(index int) string {
	return C.GoString(C.sqlite3_column_origin_name(s.stmt, C.int(index)))
}

// ColumnDeclaredType returns the declared type of the table column of a particular result column in SELECT statement. If the result column is an expression or subquery, then a NULL pointer is returned.
// The left-most column is column 0.
// (See http://www.sqlite.org/c3ref/column_decltype.html)
func (s *Stmt) ColumnDeclaredType(index int) string {
	return C.GoString(C.sqlite3_column_decltype(s.stmt, C.int(index)))
}

// See Conn.ForeignKeys
type ForeignKey struct {
	Table string
	From  []string
	To    []string
}

// ForeignKeys returns one description for each foreign key that references a column in the argument table.
// (See http://www.sqlite.org/pragma.html#pragma_foreign_key_list)
func (c *Conn) ForeignKeys(dbName, table string) (map[int]*ForeignKey, error) {
	var pragma string
	if len(dbName) == 0 {
		pragma = Mprintf("PRAGMA foreign_key_list(%Q)", table)
	} else {
		pragma = Mprintf2("PRAGMA %Q.foreign_key_list(%Q)", dbName, table)
	}
	s, err := c.prepare(pragma)
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

// Indexes returns one description for each index associated with the given table.
// (See http://www.sqlite.org/pragma.html#pragma_index_list)
func (c *Conn) Indexes(dbName, table string) ([]Index, error) {
	var pragma string
	if len(dbName) == 0 {
		pragma = Mprintf("PRAGMA index_list(%Q)", table)
	} else {
		pragma = Mprintf2("PRAGMA %Q.index_list(%Q)", dbName, table)
	}
	s, err := c.prepare(pragma)
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

// IndexColumns returns one description for each column in the named index.
// Only Column.Cid and Column.Name are specified. All other fields are unspecifed.
// (See http://www.sqlite.org/pragma.html#pragma_index_info)
func (c *Conn) IndexColumns(dbName, index string) ([]Column, error) {
	var pragma string
	if len(dbName) == 0 {
		pragma = Mprintf("PRAGMA index_info(%Q)", index)
	} else {
		pragma = Mprintf2("PRAGMA %Q.index_info(%Q)", dbName, index)
	}
	s, err := c.prepare(pragma)
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

// Mprintf is like fmt.Printf but implements some additional formatting options
// that are useful for constructing SQL statements.
// (See http://sqlite.org/c3ref/mprintf.html)
func Mprintf(format string, arg string) string {
	zSQL := mPrintf(format, arg)
	defer C.sqlite3_free(unsafe.Pointer(zSQL))
	return C.GoString(zSQL)
}
func mPrintf(format, arg string) *C.char {
	cf := C.CString(format)
	defer C.free(unsafe.Pointer(cf))
	ca := C.CString(arg)
	defer C.free(unsafe.Pointer(ca))
	return C.my_mprintf(cf, ca)
}

// Mprintf2 is like fmt.Printf but implements some additional formatting options
// that are useful for constructing SQL statements.
// (See http://sqlite.org/c3ref/mprintf.html)
func Mprintf2(format string, arg1, arg2 string) string {
	cf := C.CString(format)
	defer C.free(unsafe.Pointer(cf))
	ca1 := C.CString(arg1)
	defer C.free(unsafe.Pointer(ca1))
	ca2 := C.CString(arg2)
	defer C.free(unsafe.Pointer(ca2))
	zSQL := C.my_mprintf2(cf, ca1, ca2)
	defer C.sqlite3_free(unsafe.Pointer(zSQL))
	return C.GoString(zSQL)
}
