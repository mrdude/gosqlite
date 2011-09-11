// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqlite provides access to the SQLite library, version 3.

package sqlite

import (
	"time"
)

const (
	JULIAN_DAY         = 2440587.5 // 1970-01-01 00:00:00 is JD 2440587.5
	DAY_IN_NANOSECONDS = 60 * 60 * 24 * 10E6
)

func JulianDayToUTC(jd float64) *time.Time {
	jd -= JULIAN_DAY
	jd *= DAY_IN_NANOSECONDS
	return time.NanosecondsToUTC(int64(jd))
}
func JulianDayToLocalTime(jd float64) *time.Time {
	jd -= JULIAN_DAY
	jd *= DAY_IN_NANOSECONDS
	return time.NanosecondsToLocalTime(int64(jd))
}

func JulianDay(t *time.Time) float64 {
	ns := float64(t.Nanoseconds())
	if ns >= 0 {
		ns += 0.5
	}
	return ns/DAY_IN_NANOSECONDS + JULIAN_DAY
}
