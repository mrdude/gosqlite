// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"fmt"
)

// IntegrityCheck checks database integrity.
// Database name is optional (default is 'main').
// (See http://www.sqlite.org/pragma.html#pragma_integrity_check
// and http://www.sqlite.org/pragma.html#pragma_quick_check)
func (c *Conn) IntegrityCheck(dbName string, max int, quick bool) error {
	var prefix string
	if quick {
		prefix = "quick"
	} else {
		prefix = "integrity"
	}
	pragmaName = fmt.Sprintf("%s_check(%d)", prefix, max)
	var msg string
	err := c.OneValue(pragma(dbName, pragmaName), &msg)
	if err != nil {
		return err
	}
	if msg != "ok" {
		return c.specificError("Integrity check failed (%s)", msg)
	}
	return nil
}

// Encoding returns the text encoding used by the specified database.
// Database name is optional (default is 'main').
// (See http://sqlite.org/pragma.html#pragma_encoding)
func (c *Conn) Encoding(dbName string) (string, error) {
	var encoding string
	err := c.OneValue(pragma(dbName, "encoding"), &encoding)
	if err != nil {
		return "", err
	}
	return encoding, nil
}

// SchemaVersion gets the value of the schema-version.
// Database name is optional (default is 'main').
// (See http://sqlite.org/pragma.html#pragma_schema_version)
func (c *Conn) SchemaVersion(dbName string) (int, error) {
	var version int
	err := c.OneValue(pragma(dbName, "schema_version"), &version)
	if err != nil {
		return -1, err
	}
	return version, nil
}

// SetRecursiveTriggers sets or clears the recursive trigger capability.
// Database name is optional (default is 'main').
// (See http://sqlite.org/pragma.html#pragma_recursive_triggers)
func (c *Conn) SetRecursiveTriggers(dbName string, on bool) error {
	return c.exec(pragma(dbName, fmt.Sprintf("recursive_triggers=%t"), on))
}

func pragma(dbName, pragmaName string) string {
	if len(dbName) == 0 {
		return "PRAGMA " + pragmaName
	}
	return Mprintf("PRAGMA %Q."+pragmaName, dbName)
}
