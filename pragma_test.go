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
