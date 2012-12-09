// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	"testing"
)

func TestIntegrityCheck(t *testing.T) {
	db := open(t)
	defer db.Close()
	checkNoError(t, db.IntegrityCheck("", 1, true), "Error checking integrity of database: %s")
}

func TestEncoding(t *testing.T) {
	db := open(t)
	defer db.Close()
	encoding, err := db.Encoding("")
	checkNoError(t, err, "Error reading encoding of database: %s")
	assertEquals(t, "Expecting %s but got %s", "UTF-8", encoding)
}

func TestSchemaVersion(t *testing.T) {
	db := open(t)
	defer db.Close()
	version, err := db.SchemaVersion("")
	checkNoError(t, err, "Error reading schema version of database: %s")
	assertEquals(t, "expecting %d but got %d", 0, version)
}

func TestJournalMode(t *testing.T) {
	db := open(t)
	defer db.Close()
	mode, err := db.JournalMode("")
	checkNoError(t, err, "Error reading journaling mode of database: %s")
	assertEquals(t, "expecting %s but got %s", "memory", mode)
}

func TestSetJournalMode(t *testing.T) {
	db := open(t)
	defer db.Close()
	mode, err := db.SetJournalMode("", "OFF")
	checkNoError(t, err, "Error setting journaling mode of database: %s")
	assertEquals(t, "expecting %s but got %s", "off", mode)
}

func TestLockingMode(t *testing.T) {
	db := open(t)
	defer db.Close()
	mode, err := db.LockingMode("")
	checkNoError(t, err, "Error reading locking-mode of database: %s")
	assertEquals(t, "expecting %s but got %s", "normal", mode)
}

func TestSetLockingMode(t *testing.T) {
	db := open(t)
	defer db.Close()
	mode, err := db.SetLockingMode("", "exclusive")
	checkNoError(t, err, "Error setting locking-mode of database: %s")
	assertEquals(t, "expecting %s but got %s", "exclusive", mode)
}

func TestSynchronous(t *testing.T) {
	db := open(t)
	defer db.Close()
	mode, err := db.Synchronous("")
	checkNoError(t, err, "Error reading synchronous flag of database: %s")
	assertEquals(t, "expecting %d but got %d", 2, mode)
}

func TestSetSynchronous(t *testing.T) {
	db := open(t)
	defer db.Close()
	err := db.SetSynchronous("", 0)
	checkNoError(t, err, "Error setting synchronous flag of database: %s")
	mode, err := db.Synchronous("")
	checkNoError(t, err, "Error reading synchronous flag of database: %s")
	assertEquals(t, "expecting %d but got %d", 0, mode)
}
