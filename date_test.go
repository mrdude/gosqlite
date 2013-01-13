// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"testing"
	"time"
)

func TestJulianDay(t *testing.T) {
	utc := JulianDayToUTC(2440587.5)
	if utc.Unix() != 0 {
		t.Errorf("Error, expecting %d got %d", 0, utc.Unix())
	}
	now := time.Now()
	r := JulianDayToLocalTime(JulianDay(now))
	if r.Unix() != now.Unix() { // FIXME Rounding problem?
		t.Errorf("%#v <> %#v", now, r)
	}
}

func TestBind(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	var delta int
	err := db.OneValue("SELECT CAST(strftime('%s', 'now') AS NUMERIC) - ?", &delta, time.Now())
	checkNoError(t, err, "Error reading date: %#v")
	if delta != 0 {
		t.Errorf("Delta between Go and SQLite timestamps: %d", delta)
	}
}

func TestScan(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	var dt time.Time
	err := db.OneValue("SELECT date('now')", &dt)
	checkNoError(t, err, "Error reading date: %#v")
	if dt.IsZero() {
		t.Error("Unexpected zero date")
	}

	var tm time.Time
	err = db.OneValue("SELECT time('now')", &tm)
	checkNoError(t, err, "Error reading date: %#v")
	if tm.IsZero() {
		t.Error("Unexpected zero time")
	}

	var dtm time.Time
	err = db.OneValue("SELECT strftime('%Y-%m-%dT%H:%M:%f', 'now')", &dtm)
	checkNoError(t, err, "Error reading date: %#v")
	if dtm.IsZero() {
		t.Error("Unexpected zero datetime")
	}

	var jd time.Time
	err = db.OneValue("SELECT CAST(strftime('%J', 'now') AS NUMERIC)", &jd)
	checkNoError(t, err, "Error reading date: %#v")
	if jd.IsZero() {
		t.Error("Unexpected zero julian day")
	}

	var unix time.Time
	err = db.OneValue("SELECT CAST(strftime('%s', 'now') AS NUMERIC)", &unix)
	checkNoError(t, err, "Error reading date: %#v")
	if unix.IsZero() {
		t.Error("Unexpected zero julian day")
	}
}
