// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"fmt"
)

// Check database integrity
// (See http://www.sqlite.org/pragma.html#pragma_integrity_check
// and http://www.sqlite.org/pragma.html#pragma_quick_check)
// TODO Make possible to specify the database-name (PRAGMA %Q.integrity_check(.))
func (c *Conn) IntegrityCheck(max int, quick bool) error {
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

// Returns the text encoding used by the main database
// (See http://sqlite.org/pragma.html#pragma_encoding)
// TODO Make possible to specify the database-name (PRAGMA %Q.encoding)
func (c *Conn) Encoding() (string, error) {
	var encoding string
	err := c.OneValue("PRAGMA encoding", &encoding)
	if err != nil {
		return "", err
	}
	return encoding, nil
}

// (See http://sqlite.org/pragma.html#pragma_schema_version)
// TODO Make possible to specify the database-name (PRAGMA %Q.schema_version)
func (c *Conn) SchemaVersion() (int, error) {
	var version int
	err := c.OneValue("PRAGMA schema_version", &version)
	if err != nil {
		return -1, err
	}
	return version, nil
}
