// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	"fmt"
	"testing"

	"github.com/bmizerany/assert"
	. "github.com/gwenn/gosqlite"
)

func init() {
	err := ConfigThreadingMode(Serialized)
	if err != nil {
		panic(fmt.Sprintf("cannot change threading mode: '%s'", err))
	}
	err = ConfigMemStatus(true)
	if err != nil {
		panic(fmt.Sprintf("cannot activate mem status: '%s'", err))
	}
	err = ConfigUri(true)
	if err != nil {
		panic(fmt.Sprintf("cannot activate uri handling: '%s'", err))
	}
	err = EnableSharedCache(false)
	if err != nil {
		panic(fmt.Sprintf("couldn't disable shared cache: '%s'", err))
	}
}

func TestEnableFKey(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	b := Must(db.IsFKeyEnabled())
	if !b {
		b = Must(db.EnableFKey(true))
		assert.T(t, b, "cannot enable FK")
	}
}

func TestEnableTriggers(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	b := Must(db.AreTriggersEnabled())
	if !b {
		b = Must(db.EnableTriggers(true))
		assert.T(t, b, "cannot enable triggers")
	}
}

func TestEnableExtendedResultCodes(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	checkNoError(t, db.EnableExtendedResultCodes(true), "cannot enable extended result codes: %s")
}

func TestConnSettings(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	err := db.EnableLoadExtension(false)
	checkNoError(t, err, "EnableLoadExtension error: %s")
	err = db.SetRecursiveTriggers("main", true)
	checkNoError(t, err, "SetRecursiveTriggers error: %s")
}
