// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"fmt"
)

// Check database integrity
// Database name is optional
// (See http://www.sqlite.org/pragma.html#pragma_integrity_check
// and http://www.sqlite.org/pragma.html#pragma_quick_check)
// TODO Make possible to specify the database-name (PRAGMA %Q.integrity_check(.))
func (c *Conn) IntegrityCheck(dbName string, max int, quick bool) error {
	var pragma string
	if quick {
		pragma = "quick"
	} else {
		pragma = "integrity"
	}
	var msg string
	err := c.OneValue(fmt.Sprintf("PRAGMA %s_check(%d)", pragma, max), &msg)
	if err != nil {
		return err
	}
	if msg != "ok" {
		return c.specificError("Integrity check failed (%s)", msg)
	}
	return nil
}

// Database name is optional
// Returns the text encoding used by the main database
// (See http://sqlite.org/pragma.html#pragma_encoding)
func (c *Conn) Encoding(dbName string) (string, error) {
	var encoding string
	err := c.OneValue(pragma(dbName, "PRAGMA encoding", "PRAGMA %Q.encoding"), &encoding)
	if err != nil {
		return "", err
	}
	return encoding, nil
}

// Database name is optional
// (See http://sqlite.org/pragma.html#pragma_schema_version)
func (c *Conn) SchemaVersion(dbName string) (int, error) {
	var version int
	err := c.OneValue(pragma(dbName, "PRAGMA schema_version", "PRAGMA %Q.schema_version"), &version)
	if err != nil {
		return -1, err
	}
	return version, nil
}

// (See http://sqlite.org/pragma.html#pragma_recursive_triggers)
// TODO Make possible to specify the database-name (PRAGMA %Q.recursive_triggers=%)
func (c *Conn) SetRecursiveTriggers(on bool) error {
	return c.exec(fmt.Sprintf("PRAGMA recursive_triggers=%t", on))
}

func pragma(dbName, unqualified, qualified string) string {
	if len(dbName) == 0 {
		return unqualified
	}
	return Mprintf(qualified, dbName)
}
