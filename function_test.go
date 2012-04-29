// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"math/rand"
	"regexp"
	"testing"
)

func half(ctx *ScalarContext, nArg int) {
	nt := ctx.NumericType(0)
	if nt == Integer || nt == Float {
		ctx.ResultDouble(ctx.Double(0) / 2)
	} else {
		ctx.ResultNull()
	}
}

func TestScalarFunction(t *testing.T) {
	db := open(t)
	defer db.Close()
	err := db.CreateScalarFunction("half", 1, nil, half, nil)
	checkNoError(t, err, "couldn't create function: %s")
	var d float64
	err = db.OneValue("select half(6)", &d)
	checkNoError(t, err, "couldn't retrieve result: %s")
	assertEquals(t, "Expected %f but got %f", 3.0, d)
	err = db.CreateScalarFunction("half", 1, nil, nil, nil)
	checkNoError(t, err, "couldn't destroy function: %s")
}

var reused bool

func re(ctx *ScalarContext, nArg int) {
	ad := ctx.GetAuxData(0)
	var re *regexp.Regexp
	if ad == nil {
		reused = false
		//println("Compile")
		var err error
		re, err = regexp.Compile(ctx.Text(0))
		if err != nil {
			ctx.ResultError(err.Error())
			return
		}
		ctx.SetAuxData(0, re)
	} else {
		reused = true
		//println("Reuse")
		var ok bool
		if re, ok = ad.(*regexp.Regexp); !ok {
			println(ad)
			ctx.ResultError("AuxData not a regexp")
			return
		}
	}
	m := re.MatchString(ctx.Text(1))
	ctx.ResultBool(m)
}

// Useless (just for test)
func reDestroy(ad interface{}) {
	//println("reDestroy")
}

func TestRegexpFunction(t *testing.T) {
	db := open(t)
	defer db.Close()
	err := db.CreateScalarFunction("regexp", 2, nil, re, reDestroy)
	checkNoError(t, err, "couldn't create function: %s")
	s, err := db.Prepare("select regexp('l.s[aeiouy]', name) from (select 'lisa' as name union all select 'bart')")
	checkNoError(t, err, "couldn't prepare statement: %s")
	defer s.Finalize()

	if b := Must(s.Next()); !b {
		t.Fatalf("No result")
	}
	i, _, err := s.ScanInt(0)
	checkNoError(t, err, "couldn't scan result: %s")
	assertEquals(t, "expected %d but got %d", 1, i)
	assert(t, "unexpected reused state", !reused)

	if b := Must(s.Next()); !b {
		t.Fatalf("No result")
	}
	i, _, err = s.ScanInt(0)
	checkNoError(t, err, "couldn't scan result: %s")
	assertEquals(t, "expected %d but got %d", 0, i)
	assert(t, "unexpected reused state", reused)
}

func sumStep(ctx *AggregateContext, nArg int) {
	nt := ctx.NumericType(0)
	if nt == Integer || nt == Float {
		var sum int64
		var ok bool
		if sum, ok = (ctx.Aggregate).(int64); !ok {
			sum = 0
		}
		sum += ctx.Int64(0)
		ctx.Aggregate = sum
	}
}

func sumFinal(ctx *AggregateContext) {
	if sum, ok := (ctx.Aggregate).(int64); ok {
		ctx.ResultInt64(sum)
	} else {
		ctx.ResultNull()
	}
}

func TestSumFunction(t *testing.T) {
	db := open(t)
	defer db.Close()
	err := db.CreateAggregateFunction("mysum", 1, nil, sumStep, sumFinal, nil)
	checkNoError(t, err, "couldn't create function: %s")
	var i int
	err = db.OneValue("select mysum(i) from (select 2 as i union all select 2)", &i)
	checkNoError(t, err, "couldn't execute statement: %s")
	assertEquals(t, "expected %d but got %v", 4, i)
}

func randomFill(db *Conn, n int) {
	db.Exec("DROP TABLE IF EXISTS test")
	db.Exec("CREATE TABLE test (name TEXT, rank int)")
	s, _ := db.Prepare("INSERT INTO test (name, rank) VALUES (?, ?)")
	defer s.Finalize()

	names := []string{"Bart", "Homer", "Lisa", "Maggie", "Marge"}

	db.Begin()
	for i := 0; i < n; i++ {
		s.Exec(names[rand.Intn(len(names))], rand.Intn(100))
	}
	db.Commit()
}

func BenchmarkLike(b *testing.B) {
	b.StopTimer()
	db, _ := Open(":memory:")
	defer db.Close()
	randomFill(db, 1)
	cs, _ := db.Prepare("SELECT count(1) FROM test where name like 'lisa'")
	defer cs.Finalize()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		Must(cs.Next())
		cs.Reset()
	}
}

func BenchmarkHalf(b *testing.B) {
	b.StopTimer()
	db, _ := Open(":memory:")
	defer db.Close()
	randomFill(db, 1)
	db.CreateScalarFunction("half", 1, nil, half, nil)
	cs, _ := db.Prepare("SELECT count(1) FROM test where half(rank) > 20")
	defer cs.Finalize()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		Must(cs.Next())
		cs.Reset()
	}
}

func BenchmarkRegexp(b *testing.B) {
	b.StopTimer()
	db, _ := Open(":memory:")
	defer db.Close()
	randomFill(db, 1)
	db.CreateScalarFunction("regexp", 2, nil, re, reDestroy)
	cs, _ := db.Prepare("SELECT count(1) FROM test where name regexp '(?i)\\blisa\\b'")
	defer cs.Finalize()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		Must(cs.Next())
		cs.Reset()
	}
}
