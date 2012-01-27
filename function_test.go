package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"math/rand"
	"regexp"
	"testing"
)

func half(ctx *Context, nArg int) {
	nt := ctx.NumericType(0)
	if nt == Integer || nt == Float {
		ctx.ResultDouble(ctx.Double(0) / 2)
	} else {
		ctx.ResultNull()
	}
}

func TestScalarFunction(t *testing.T) {
	db, err := Open("")
	checkNoError(t, err, "couldn't open database file: %s")
	defer db.Close()
	err = db.CreateScalarFunction("half", 1, nil, half, nil)
	checkNoError(t, err, "couldn't create function: %s")
	d, err := db.OneValue("select half(6)")
	checkNoError(t, err, "couldn't retrieve result: %s")
	if d != 3.0 {
		t.Errorf("Expected %f but got %f", 3.0, d)
	}
	err = db.CreateScalarFunction("half", 1, nil, nil, nil)
	checkNoError(t, err, "couldn't destroy function: %s")
}

func re(ctx *Context, nArg int) {
	ad := ctx.GetAuxData(0)
	var re *regexp.Regexp
	if ad == nil {
		//println("Compile")
		var err error
		re, err = regexp.Compile(ctx.Text(0))
		if err != nil {
			ctx.ResultError(err.Error())
			return
		}
		ctx.SetAuxData(0, re)
	} else {
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
	db, err := Open("")
	checkNoError(t, err, "couldn't open database file: %s")
	defer db.Close()
	err = db.CreateScalarFunction("regexp", 2, nil, re, reDestroy)
	checkNoError(t, err, "couldn't create function: %s")
	s, err := db.Prepare("select regexp('l.s[aeiouy]', name) from (select 'lisa' as name union all select 'bart')")
	checkNoError(t, err, "couldn't prepare statement: %s")
	defer s.Finalize()
	if b := Must(s.Next()); !b {
		t.Fatalf("No result")
	}
	i, _, err := s.ScanInt(0)
	checkNoError(t, err, "couldn't scan result: %s")
	if i != 1 {
		t.Errorf("Expected %d but got %d", 1, i)
	}
	if b := Must(s.Next()); !b {
		t.Fatalf("No result")
	}
	i, _, err = s.ScanInt(0)
	checkNoError(t, err, "couldn't scan result: %s")
	if i != 0 {
		t.Errorf("Expected %d but got %d", 0, i)
	}
}

/*
func sumStep(ctx *Context, nArg int) {
	nt := ctx.NumericType(0)
	if nt == Integer || nt == Float {
		var sum float64
		var ok bool
		if sum, ok = (ctx.AggregateContext).(float64); !ok {
			sum = 0
		}
		sum += ctx.Double(0)
		ctx.AggregateContext = sum
	}
}

func sumFinal(ctx *Context) {
	if sum, ok := (ctx.AggregateContext).(float64); ok {
		ctx.ResultDouble(sum)
	} else {
		ctx.ResultNull()
	}
}

func TestSumFunction(t *testing.T) {
	db, err := Open("")
	checkNoError(t, err, "couldn't open database file: %s")
	defer db.Close()
	err = db.CreateAggregateFunction("mysum", 1, nil, sumStep, sumFinal, nil)
	checkNoError(t, err, "couldn't create function: %s")
	i, err := db.OneValue("select sum(i) from (select 2 as i union all select 2)")
	checkNoError(t, err, "couldn't execute statement: %s")
	if i != int64(4) {
		t.Errorf("Expected %d but got %d", 4, i)
	}
}
*/

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
	db, _ := Open("")
	defer db.Close()
	randomFill(db, 1000)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cs, _ := db.Prepare("SELECT count(1) FROM test where name like 'lisa'")
		Must(cs.Next())
		cs.Finalize()
	}
}

func BenchmarkHalf(b *testing.B) {
	b.StopTimer()
	db, _ := Open("")
	defer db.Close()
	randomFill(db, 1000)
	db.CreateScalarFunction("half", 1, nil, half, nil)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cs, _ := db.Prepare("SELECT count(1) FROM test where half(rank) > 20")
		Must(cs.Next())
		cs.Finalize()
	}
}

func BenchmarkRegexp(b *testing.B) {
	b.StopTimer()
	db, _ := Open("")
	defer db.Close()
	randomFill(db, 1000)
	db.CreateScalarFunction("regexp", 2, nil, re, reDestroy)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cs, _ := db.Prepare("SELECT count(1) FROM test where name regexp  '(?i)\\blisa\\b'")
		Must(cs.Next())
		cs.Finalize()
	}
}
