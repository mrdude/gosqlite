// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"time"
)

const (
	JULIAN_DAY     = 2440587.5 // 1970-01-01 00:00:00 is JD 2440587.5
	DAY_IN_SECONDS = 60 * 60 * 24
)

// JulianDayToUTC transforms a julian day number into an UTC Time.
func JulianDayToUTC(jd float64) time.Time {
	jd -= JULIAN_DAY
	jd *= DAY_IN_SECONDS
	return time.Unix(int64(jd), 0).UTC()
}

// JulianDayToLocalTime transforms a julian day number into a local Time.
func JulianDayToLocalTime(jd float64) time.Time {
	jd -= JULIAN_DAY
	jd *= DAY_IN_SECONDS
	return time.Unix(int64(jd), 0)
}

// JulianDay converts a Time into a julian day number.
func JulianDay(t time.Time) float64 {
	ns := float64(t.Unix())
	if ns >= 0 {
		ns += 0.5
	}
	return ns/DAY_IN_SECONDS + JULIAN_DAY
}
