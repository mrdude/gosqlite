// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

/*
#include <sqlite3.h>
#include <stdlib.h>

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
	"strings"
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
	var databases = make(map[string]string)
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

// Tables returns tables (no view) from 'sqlite_master'/'sqlite_temp_master' and filters system tables out.
func (c *Conn) Tables(dbName string, temp bool) ([]string, error) {
	var sql string
	if len(dbName) == 0 {
		sql = "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY 1"
	} else {
		sql = Mprintf("SELECT name FROM %Q.sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%%' ORDER BY 1", dbName)
	}
	if temp {
		sql = strings.Replace(sql, "sqlite_master", "sqlite_temp_master", 1)
	}
	s, err := c.prepare(sql)
	if err != nil {
		return nil, err
	}
	defer s.finalize()
	var tables = make([]string, 0, 20)
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

// Views returns views from 'sqlite_master'/'sqlite_temp_master'.
func (c *Conn) Views(dbName string, temp bool) ([]string, error) {
	var sql string
	if len(dbName) == 0 {
		sql = "SELECT name FROM sqlite_master WHERE type = 'view' ORDER BY 1"
	} else {
		sql = Mprintf("SELECT name FROM %Q.sqlite_master WHERE type = 'view' ORDER BY 1", dbName)
	}
	if temp {
		sql = strings.Replace(sql, "sqlite_master", "sqlite_temp_master", 1)
	}
	s, err := c.prepare(sql)
	if err != nil {
		return nil, err
	}
	defer s.finalize()
	var views = make([]string, 0, 20)
	err = s.Select(func(s *Stmt) (err error) {
		name, _ := s.ScanText(0)
		views = append(views, name)
		return
	})
	if err != nil {
		return nil, err
	}
	return views, nil
}

// Indexes returns indexes from 'sqlite_master'/'sqlite_temp_master'.
func (c *Conn) Indexes(dbName string, temp bool) (map[string]string, error) {
	var sql string
	if len(dbName) == 0 {
		sql = "SELECT name, tbl_name FROM sqlite_master WHERE type = 'index'"
	} else {
		sql = Mprintf("SELECT name, tbl_name FROM %Q.sqlite_master WHERE type = 'index'", dbName)
	}
	if temp {
		sql = strings.Replace(sql, "sqlite_master", "sqlite_temp_master", 1)
	}
	s, err := c.prepare(sql)
	if err != nil {
		return nil, err
	}
	defer s.finalize()
	var indexes = make(map[string]string)
	var name, table string
	err = s.Select(func(s *Stmt) (err error) {
		s.Scan(&name, &table)
		indexes[name] = table
		return
	})
	if err != nil {
		return nil, err
	}
	return indexes, nil
}

// Column is the description of one table's column
// See Conn.Columns/IndexColumns
type Column struct {
	Cid       int
	Name      string
	DataType  string
	NotNull   bool
	DfltValue string // FIXME type ?
	Pk        int
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
	var columns = make([]Column, 0, 20)
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
	return &Column{-1, columnName, C.GoString(zDataType), notNull == 1, "", int(primaryKey),
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

// ColumnDeclaredType returns the declared type of the table column of a particular result column in SELECT statement.
// If the result column is an expression or subquery, then an empty string is returned.
// The left-most column is column 0.
// (See http://www.sqlite.org/c3ref/column_decltype.html)
func (s *Stmt) ColumnDeclaredType(index int) string {
	return C.GoString(C.sqlite3_column_decltype(s.stmt, C.int(index)))
}

// SQLite column type affinity
type Affinity string

const (
	Integral  = Affinity("INTEGER")
	Real      = Affinity("REAL")
	Numerical = Affinity("NUMERIC")
	None      = Affinity("NONE")
	Textual   = Affinity("TEXT")
)

// ColumnTypeAffinity returns the type affinity of the table column of a particular result column in SELECT statement.
// If the result column is an expression or subquery, then None is returned.
// The left-most column is column 0.
// (See http://sqlite.org/datatype3.html)
func (s *Stmt) ColumnTypeAffinity(index int) Affinity {
	if s.affinities == nil {
		count := s.ColumnCount()
		s.affinities = make([]Affinity, count)
	} else {
		if affinity := s.affinities[index]; affinity != "" {
			return affinity
		}
	}
	declType := s.ColumnDeclaredType(index)
	if declType == "" {
		s.affinities[index] = None
		return None
	}
	declType = strings.ToUpper(declType)
	if strings.Contains(declType, "INT") {
		s.affinities[index] = Integral
		return Integral
	} else if strings.Contains(declType, "TEXT") || strings.Contains(declType, "CHAR") || strings.Contains(declType, "CLOB") {
		s.affinities[index] = Textual
		return Textual
	} else if strings.Contains(declType, "BLOB") {
		s.affinities[index] = None
		return None
	} else if strings.Contains(declType, "REAL") || strings.Contains(declType, "FLOA") || strings.Contains(declType, "DOUB") {
		s.affinities[index] = Real
		return Real
	}
	s.affinities[index] = Numerical
	return Numerical
}

// ForeignKey is the description of one table's foreign key
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

// Index is the description of one table's index
// See Conn.Indexes
type Index struct {
	Name   string
	Unique bool
}

// TableIndexes returns one description for each index associated with the given table.
// (See http://www.sqlite.org/pragma.html#pragma_index_list)
func (c *Conn) TableIndexes(dbName, table string) ([]Index, error) {
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
	var indexes = make([]Index, 0, 5)
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
// Only Column.Cid and Column.Name are specified. All other fields are unspecified.
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
	var columns = make([]Column, 0, 5)
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
