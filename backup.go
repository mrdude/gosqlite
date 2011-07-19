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

func NewBackup(dst *Conn, dstTable string, src *Conn, srcTable string) (*Backup, os.Error) {
	dname := C.CString(dstTable)
	sname := C.CString(srcTable)
	defer C.free(unsafe.Pointer(dname))
	defer C.free(unsafe.Pointer(sname))

	sb := C.sqlite3_backup_init(dst.db, dname, src.db, sname)
	if sb == nil {
		return nil, dst.error(C.sqlite3_errcode(dst.db))
	}
	return &Backup{sb, dst, src}, nil
}

type Backup struct {
	sb       *C.sqlite3_backup
	dst, src *Conn
}

func (b *Backup) Step(npage int) os.Error {
	rv := C.sqlite3_backup_step(b.sb, C.int(npage))
	if rv == 0 || Errno(rv) == ErrBusy || Errno(rv) == ErrLocked {
		return nil
	}
	return Errno(rv)
}

type BackupStatus struct {
	Remaining int
	PageCount int
}

func (b *Backup) Status() BackupStatus {
	return BackupStatus{int(C.sqlite3_backup_remaining(b.sb)), int(C.sqlite3_backup_pagecount(b.sb))}
}

func (b *Backup) Run(npage int, sleepNs int64, c chan<- BackupStatus) os.Error {
	var err os.Error
	for {
		err = b.Step(npage)
		if err != nil {
			break
		}
		if c != nil {
			c <- b.Status()
		}
		time.Sleep(sleepNs)
	}
	return b.dst.error(C.sqlite3_errcode(b.dst.db))
}

func (b *Backup) Close() os.Error {
	if b.sb == nil {
		return os.EINVAL
	}
	C.sqlite3_backup_finish(b.sb)
	b.sb = nil
	return nil
}
