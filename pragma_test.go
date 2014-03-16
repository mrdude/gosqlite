// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	"io"
	"testing"

	"github.com/bmizerany/assert"
	. "github.com/gwenn/gosqlite"
)

func TestIntegrityCheck(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	checkNoError(t, db.IntegrityCheck("", 1, true), "Error checking integrity of database: %s")
}

func TestEncoding(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	encoding, err := db.Encoding("")
	checkNoError(t, err, "Error reading encoding of database: %s")
	assert.Equal(t, "UTF-8", encoding)
}

func TestSchemaVersion(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	version, err := db.SchemaVersion("")
	checkNoError(t, err, "Error reading schema version of database: %s")
	assert.Equal(t, 0, version)
}

func TestJournalMode(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	mode, err := db.JournalMode("")
	checkNoError(t, err, "Error reading journaling mode of database: %s")
	assert.Equal(t, "memory", mode)
}

func TestSetJournalMode(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	mode, err := db.SetJournalMode("", "OFF")
	checkNoError(t, err, "Error setting journaling mode of database: %s")
	assert.Equal(t, "off", mode)
}

func TestLockingMode(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	mode, err := db.LockingMode("")
	checkNoError(t, err, "Error reading locking-mode of database: %s")
	assert.Equal(t, "normal", mode)
}

func TestSetLockingMode(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	mode, err := db.SetLockingMode("", "exclusive")
	checkNoError(t, err, "Error setting locking-mode of database: %s")
	assert.Equal(t, "exclusive", mode)
}

func TestSynchronous(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	mode, err := db.Synchronous("")
	checkNoError(t, err, "Error reading synchronous flag of database: %s")
	assert.Equal(t, 2, mode)
}

func TestSetSynchronous(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	err := db.SetSynchronous("", 0)
	checkNoError(t, err, "Error setting synchronous flag of database: %s")
	mode, err := db.Synchronous("")
	checkNoError(t, err, "Error reading synchronous flag of database: %s")
	assert.Equal(t, 0, mode)
}

func TestQueryOnly(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	mode, err := db.QueryOnly("")
	if err == io.EOF {
		return // not supported
	}
	checkNoError(t, err, "Error reading query_only status of database: %s")
	assert.T(t, !mode, "expecting query_only to be false by default")
	err = db.SetQueryOnly("", true)
	checkNoError(t, err, "Error setting query_only status of database: %s")
	err = db.Exec("CREATE TABLE test (data TEXT)")
	assert.T(t, err != nil, "expected error")
	//println(err.Error())
}

func TestApplicationId(t *testing.T) {
	if VersionNumber() < 3007017 {
		return
	}

	db := open(t)
	defer checkClose(db, t)

	appId, err := db.ApplicationId("")
	checkNoError(t, err, "error getting application Id: %s")
	assert.Equalf(t, 0, appId, "got: %d; want: %d", appId, 0)

	err = db.SetApplicationId("", 123)
	checkNoError(t, err, "error setting application Id: %s")

	appId, err = db.ApplicationId("")
	checkNoError(t, err, "error getting application Id: %s")
	assert.Equalf(t, 123, appId, "got: %d; want: %d", appId, 123)
}

func TestForeignKeyCheck(t *testing.T) {
	if VersionNumber() < 3007016 {
		return
	}

	db := open(t)
	defer checkClose(db, t)
	checkNoError(t, db.Exec(`
		CREATE TABLE tree (
		id INTEGER PRIMARY KEY NOT NULL,
		parentId INTEGER,
		name TEXT NOT NULL,
		FOREIGN KEY (parentId) REFERENCES tree(id)
		);
	  INSERT INTO tree VALUES (0, NULL, 'root'),
	  (1, 0, 'node1'),
	  (2, 0, 'node2'),
	  (3, 1, 'leaf'),
	  (4, 5, 'orphan')
	  ;
	`), "%s")
	vs, err := db.ForeignKeyCheck("", "tree")
	checkNoError(t, err, "error while checking FK: %s")
	assert.Equal(t, 1, len(vs), "one FK violation expected")
	v := vs[0]
	assert.Equal(t, FkViolation{Table: "tree", RowId: 4, Parent: "tree", FkId: 0}, v)
	fks, err := db.ForeignKeys("", "tree")
	checkNoError(t, err, "error while loading FK: %s")
	fk, ok := fks[v.FkId]
	assert.Tf(t, ok, "no FK with id: %d", v.FkId)
	assert.Equal(t, &ForeignKey{Table: "tree", From: []string{"parentId"}, To: []string{"id"}}, fk)
}
