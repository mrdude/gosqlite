// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqlite provides access to the SQLite library, version 3.
package sqlite

/*
#cgo LDFLAGS: -lsqlite3

#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>

// These wrappers are necessary because SQLITE_TRANSIENT
// is a pointer constant, and cgo doesn't translate them correctly.
// The definition in sqlite3.h is:
//
// typedef void (*sqlite3_destructor_type)(void*);
// #define SQLITE_STATIC      ((sqlite3_destructor_type)0)
// #define SQLITE_TRANSIENT   ((sqlite3_destructor_type)-1)

static int my_bind_text(sqlite3_stmt *stmt, int n, char *p, int np) {
	return sqlite3_bind_text(stmt, n, p, np, SQLITE_TRANSIENT);
}
static int my_bind_blob(sqlite3_stmt *stmt, int n, void *p, int np) {
	return sqlite3_bind_blob(stmt, n, p, np, SQLITE_TRANSIENT);
}
*/
import "C"

import (
	"fmt"
	"os"
	"reflect"
	"unsafe"
)

type Errno int

func (e Errno) String() string {
	s := errText[e]
	if s == "" {
		return fmt.Sprintf("errno %d", int(e))
	}
	return s
}

var (
	ErrError      os.Error = Errno(1)   //    /* SQL error or missing database */
	ErrInternal   os.Error = Errno(2)   //    /* Internal logic error in SQLite */
	ErrPerm       os.Error = Errno(3)   //    /* Access permission denied */
	ErrAbort      os.Error = Errno(4)   //    /* Callback routine requested an abort */
	ErrBusy       os.Error = Errno(5)   //    /* The database file is locked */
	ErrLocked     os.Error = Errno(6)   //    /* A table in the database is locked */
	ErrNoMem      os.Error = Errno(7)   //    /* A malloc() failed */
	ErrReadOnly   os.Error = Errno(8)   //    /* Attempt to write a readonly database */
	ErrInterrupt  os.Error = Errno(9)   //    /* Operation terminated by sqlite3_interrupt()*/
	ErrIOErr      os.Error = Errno(10)  //    /* Some kind of disk I/O error occurred */
	ErrCorrupt    os.Error = Errno(11)  //    /* The database disk image is malformed */
	ErrFull       os.Error = Errno(13)  //    /* Insertion failed because database is full */
	ErrCantOpen   os.Error = Errno(14)  //    /* Unable to open the database file */
	ErrEmpty      os.Error = Errno(16)  //    /* Database is empty */
	ErrSchema     os.Error = Errno(17)  //    /* The database schema changed */
	ErrTooBig     os.Error = Errno(18)  //    /* String or BLOB exceeds size limit */
	ErrConstraint os.Error = Errno(19)  //    /* Abort due to constraint violation */
	ErrMismatch   os.Error = Errno(20)  //    /* Data type mismatch */
	ErrMisuse     os.Error = Errno(21)  //    /* Library used incorrectly */
	ErrNolfs      os.Error = Errno(22)  //    /* Uses OS features not supported on host */
	ErrAuth       os.Error = Errno(23)  //    /* Authorization denied */
	ErrFormat     os.Error = Errno(24)  //    /* Auxiliary database format error */
	ErrRange      os.Error = Errno(25)  //    /* 2nd parameter to sqlite3_bind out of range */
	ErrNotDB      os.Error = Errno(26)  //    /* File opened that is not a database file */
	Row                    = Errno(100) //   /* sqlite3_step() has another row ready */
	Done                   = Errno(101) //   /* sqlite3_step() has finished executing */
)

var errText = map[Errno]string{
	1:   "SQL error or missing database",
	2:   "Internal logic error in SQLite",
	3:   "Access permission denied",
	4:   "Callback routine requested an abort",
	5:   "The database file is locked",
	6:   "A table in the database is locked",
	7:   "A malloc() failed",
	8:   "Attempt to write a readonly database",
	9:   "Operation terminated by sqlite3_interrupt()*/",
	10:  "Some kind of disk I/O error occurred",
	11:  "The database disk image is malformed",
	12:  "NOT USED. Table or record not found",
	13:  "Insertion failed because database is full",
	14:  "Unable to open the database file",
	15:  "NOT USED. Database lock protocol error",
	16:  "Database is empty",
	17:  "The database schema changed",
	18:  "String or BLOB exceeds size limit",
	19:  "Abort due to constraint violation",
	20:  "Data type mismatch",
	21:  "Library used incorrectly",
	22:  "Uses OS features not supported on host",
	23:  "Authorization denied",
	24:  "Auxiliary database format error",
	25:  "2nd parameter to sqlite3_bind out of range",
	26:  "File opened that is not a database file",
	100: "sqlite3_step() has another row ready",
	101: "sqlite3_step() has finished executing",
}

func (c *Conn) error(rv C.int) os.Error {
	if c == nil || c.db == nil {
		return os.NewError("nil sqlite database")
	}
	if rv == 0 {
		return nil
	}
	if rv == 21 { // misuse
		return Errno(rv)
	}
	return os.NewError(Errno(rv).String() + ": " + C.GoString(C.sqlite3_errmsg(c.db)))
}

type Conn struct {
	db *C.sqlite3
}

func Version() string {
	p := C.sqlite3_libversion()
	return C.GoString(p)
}

func EnableSharedCache(b bool) os.Error {
	rv := C.sqlite3_enable_shared_cache(btocint(b))
	if rv != 0 {
		return Errno(rv)
	}
	return nil
}

// ':memory:' for memory db
// '' for temp db
func Open(filename string) (*Conn, os.Error) {
	if C.sqlite3_threadsafe() == 0 {
		return nil, os.NewError("sqlite library was not compiled for thread-safe operation")
	}

	var db *C.sqlite3
	name := C.CString(filename)
	defer C.free(unsafe.Pointer(name))
	rv := C.sqlite3_open_v2(name, &db,
		C.SQLITE_OPEN_FULLMUTEX|
			C.SQLITE_OPEN_READWRITE|
			C.SQLITE_OPEN_CREATE|
			C.SQLITE_OPEN_URI,
		nil)
	if rv != 0 {
		if db != nil {
			C.sqlite3_close(db)
		}
		return nil, Errno(rv)
	}
	if db == nil {
		return nil, os.NewError("sqlite succeeded without returning a database")
	}
	return &Conn{db}, nil
}

func (c *Conn) BusyTimeout(ms int) os.Error {
	rv := C.sqlite3_busy_timeout(c.db, C.int(ms))
	if rv == 0 {
		return nil
	}
	return Errno(rv)
}

// Don't use it with SELECT or anything that returns data.
func (c *Conn) Exec(cmd string, args ...interface{}) os.Error {
	for len(cmd) > 0 {
		s, err := c.Prepare(cmd)
		if err != nil {
			return err
		} else if s.stmt == nil {
			// this happens for a comment or white-space
			cmd = s.tail
			s.Finalize()
			continue
		}
		err = s.Exec(args...)
		if err != nil {
			s.Finalize()
			return err
		}
		if len(s.tail) > 0 {
			if len(args) > 0 {
				s.Finalize()
				return os.NewError("Cannot execute multiple statements when args are specified")
			}
		}
		cmd = s.tail
		s.Finalize()
	}
	return nil
}

func (c *Conn) Changes() int {
	return int(C.sqlite3_changes(c.db))
}

func (c *Conn) LastInsertRowid() int64 {
	return int64(C.sqlite3_last_insert_rowid(c.db))
}

func (c *Conn) Interrupt() {
	C.sqlite3_interrupt(c.db)
}

type Stmt struct {
	c    *Conn
	stmt *C.sqlite3_stmt
	tail string
	cols map[string]int // cached columns index by name
}

func (c *Conn) Prepare(cmd string, args ...interface{}) (*Stmt, os.Error) {
	if c == nil || c.db == nil {
		return nil, os.NewError("nil sqlite database")
	}
	cmdstr := C.CString(cmd)
	defer C.free(unsafe.Pointer(cmdstr))
	var stmt *C.sqlite3_stmt
	var tail *C.char
	rv := C.sqlite3_prepare_v2(c.db, cmdstr, -1, &stmt, &tail)
	if rv != 0 {
		return nil, c.error(rv)
	}
	var t string
	if tail != nil && C.strlen(tail) > 0 {
		t = C.GoString(tail)
	}
	s := &Stmt{c: c, stmt: stmt, tail: t}
	if len(args) > 0 {
		err := s.Bind(args)
		if err != nil {
			return s, err
		}
	}
	return s, nil
}

// Don't use it with SELECT or anything that returns data.
func (s *Stmt) Exec(args ...interface{}) os.Error {
	err := s.Bind(args...)
	if err != nil {
		return err
	}
	rv := C.sqlite3_step(s.stmt)
	if Errno(rv) != Done {
		return s.c.error(rv)
	}
	return nil
}

func (s *Stmt) BindParameterCount() int {
	return int(C.sqlite3_bind_parameter_count(s.stmt))
}

func (s *Stmt) BindParameterIndex(name string) int {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	return int(C.sqlite3_bind_parameter_index(s.stmt, cname))
}

func (s *Stmt) Bind(args ...interface{}) os.Error {
	err := s.Reset()
	if err != nil {
		return err
	}

	n := s.BindParameterCount()
	if n != len(args) {
		return os.NewError(fmt.Sprintf("incorrect argument count for Stmt.Bind: have %d want %d", len(args), n))
	}

	for i, v := range args {
		var rv C.int
		index := C.int(i + 1)
		switch v := v.(type) {
		case nil:
			rv = C.sqlite3_bind_null(s.stmt, index)
		case string:
			cstr := C.CString(v)
			rv = C.my_bind_text(s.stmt, index, cstr, C.int(len(v)))
			C.free(unsafe.Pointer(cstr))
		case int:
			rv = C.sqlite3_bind_int(s.stmt, index, C.int(v))
		case int64:
			rv = C.sqlite3_bind_int64(s.stmt, index, C.sqlite3_int64(v))
		case byte:
			rv = C.sqlite3_bind_int(s.stmt, index, C.int(v))
		case bool:
			rv = C.sqlite3_bind_int(s.stmt, index, btocint(v))
		case float32:
			rv = C.sqlite3_bind_double(s.stmt, index, C.double(v))
		case float64:
			rv = C.sqlite3_bind_double(s.stmt, index, C.double(v))
		case []byte:
			var p *byte
			if len(v) > 0 {
				p = &v[0]
			}
			rv = C.my_bind_blob(s.stmt, index, unsafe.Pointer(p), C.int(len(v)))
		}
		if rv != 0 {
			return s.c.error(rv)
		}
	}
	return nil
}

func (s *Stmt) Next() (bool, os.Error) {
	rv := C.sqlite3_step(s.stmt)
	err := Errno(rv)
	if err == Row {
		return true, nil
	}
	if err != Done {
		return false, s.c.error(rv)
	}
	return false, nil
}

func (s *Stmt) Reset() os.Error {
	rv := C.sqlite3_reset(s.stmt)
	if rv != 0 {
		return s.c.error(rv)
	}
	return nil
}

func (s *Stmt) ColumnCount() int {
	return int(C.sqlite3_column_count(s.stmt))
}

// The leftmost column is number 0.
func (s *Stmt) ColumnName(index int) string {
	return C.GoString(C.sqlite3_column_name(s.stmt, C.int(index)))
}

func (s *Stmt) NamedScan(args ...interface{}) os.Error {
	if len(args)%2 != 0 {
		return os.NewError("Expected an even number of arguments")
	}
	for i := 0; i < len(args); i += 2 {
		name, ok := args[i].(string)
		if !ok {
			return os.NewError("non-string field name field")
		}
		index, err := s.fieldIndex(name) // How to look up only once for one statement ?
		if err != nil {
			return err
		}
		ptr := args[i+1]
		err = s.scanField(index, ptr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Stmt) Scan(args ...interface{}) os.Error {
	n := s.ColumnCount()
	if n != len(args) {
		return os.NewError(fmt.Sprintf("incorrect argument count for Stmt.Scan: have %d want %d", len(args), n))
	}

	for i, v := range args {
		err := s.scanField(i, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Stmt) fieldIndex(name string) (int, os.Error) {
	if s.cols == nil {
		count := s.ColumnCount()
		s.cols = make(map[string]int, count)
		for i := 0; i < count; i++ {
			s.cols[s.ColumnName(i)] = i
		}
	}
	index, ok := s.cols[name]
	if ok {
		return index, nil
	}
	return 0, os.NewError("invalid column name: " + name)
}

func (s *Stmt) scanField(index int, value interface{}) os.Error {
	switch value := value.(type) {
	case *string:
		p := C.sqlite3_column_text(s.stmt, C.int(index))
		if p == nil {
			value = nil
		} else {
			n := C.sqlite3_column_bytes(s.stmt, C.int(index))
			*value = C.GoStringN((*C.char)(unsafe.Pointer(p)), n)
		}
	case *int:
		*value = int(C.sqlite3_column_int(s.stmt, C.int(index)))
	case *int64:
		*value = int64(C.sqlite3_column_int64(s.stmt, C.int(index)))
	case *byte:
		*value = byte(C.sqlite3_column_int(s.stmt, C.int(index)))
	case *bool:
		*value = C.sqlite3_column_int(s.stmt, C.int(index)) == 1
	case *float64:
		*value = float64(C.sqlite3_column_double(s.stmt, C.int(index)))
	case *[]byte:
		p := C.sqlite3_column_blob(s.stmt, C.int(index))
		if p == nil {
			value = nil
		} else {
			n := C.sqlite3_column_bytes(s.stmt, C.int(index))
			*value = (*[1 << 30]byte)(unsafe.Pointer(p))[0:n]
		}
	default:
		return os.NewError("unsupported type in Scan: " + reflect.TypeOf(value).String())
	}
	return nil
}
func (s *Stmt) Finalize() os.Error {
	rv := C.sqlite3_finalize(s.stmt)
	if rv != 0 {
		return s.c.error(rv)
	}
	s.stmt = nil
	return nil
}

func (c *Conn) Close() os.Error {
	if c == nil || c.db == nil {
		return os.NewError("nil sqlite database")
	}
	rv := C.sqlite3_close(c.db)
	if rv != 0 {
		return c.error(rv)
	}
	c.db = nil
	return nil
}

func btocint(b bool) C.int {
	if b {
		return 1
	}
	return 0
}
