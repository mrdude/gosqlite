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
	"time"
	"unsafe"
)

// Backup/Copy the content of one database (source) to another (destination).
// Example:
//	bck, err := sqlite.NewBackup(dst, "main", src, "main")
//	// check err
//  defer bck.Close()
//	cbs := make(chan sqlite.BackupStatus)
//	go func() {
//		s := <- cbs
//		// report progress
//	}()
//	err = bck.Run(100, 250000, cbs)
//	check(err)
//
// Calls http://sqlite.org/c3ref/backup_finish.html#sqlite3backupinit
func NewBackup(dst *Conn, dstDbName string, src *Conn, srcDbName string) (*Backup, error) {
	dname := C.CString(dstDbName)
	sname := C.CString(srcDbName)
	defer C.free(unsafe.Pointer(dname))
	defer C.free(unsafe.Pointer(sname))

	sb := C.sqlite3_backup_init(dst.db, dname, src.db, sname)
	if sb == nil {
		return nil, dst.error(C.sqlite3_errcode(dst.db))
	}
	return &Backup{sb, dst, src}, nil
}

// Encapsulates backup API
type Backup struct {
	sb       *C.sqlite3_backup
	dst, src *Conn
}

// Calls http://sqlite.org/c3ref/backup_finish.html#sqlite3backupstep
func (b *Backup) Step(npage int) error {
	rv := C.sqlite3_backup_step(b.sb, C.int(npage))
	if rv == C.SQLITE_OK || Errno(rv) == ErrBusy || Errno(rv) == ErrLocked {
		return nil
	}
	return Errno(rv)
}

// Backup progression
type BackupStatus struct {
	Remaining int
	PageCount int
}

// Calls http://sqlite.org/c3ref/backup_finish.html#sqlite3backupremaining
func (b *Backup) Status() BackupStatus {
	return BackupStatus{int(C.sqlite3_backup_remaining(b.sb)), int(C.sqlite3_backup_pagecount(b.sb))}
}

// Calls http://sqlite.org/c3ref/backup_finish.html#sqlite3backupstep, sqlite3_backup_remaining and sqlite3_backup_pagecount
func (b *Backup) Run(npage int, sleepNs int64, c chan<- BackupStatus) error {
	var err error
	for {
		err = b.Step(npage)
		if err != nil {
			break
		}
		if c != nil {
			c <- b.Status()
		}
		if sleepNs > 0 {
			time.Sleep(sleepNs)
		}
	}
	return b.dst.error(C.sqlite3_errcode(b.dst.db))
}

// Calls http://sqlite.org/c3ref/backup_finish.html#sqlite3backupfinish
func (b *Backup) Close() error {
	if b.sb == nil {
		return os.EINVAL
	}
	C.sqlite3_backup_finish(b.sb)
	b.sb = nil
	return nil
}
