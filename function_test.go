package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
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
	if err != nil {
		t.Fatalf("couldn't open database file: %s", err)
	}
	defer db.Close()
	err = db.CreateScalarFunction("half", 1, nil, half, nil)
	if err != nil {
		t.Fatalf("couldn't create function: %s", err)
	}
	s, err := db.Prepare("select half(6)")
	if err != nil {
		t.Fatalf("couldn't prepare statement: %s", err)
	}
	b, err := s.Next()
	if err != nil {
		t.Fatalf("couldn't step statement: %s", err)
	}
	if !b {
		t.Fatalf("No result")
	}
	d, _, err := s.ScanDouble(0)
	if err != nil {
		t.Fatalf("couldn't scan result: %s", err)
	}
	if d != 3 {
		t.Errorf("Expected %f but got %f", 3, d)
	}
	err = s.Finalize()
	if err != nil {
		t.Fatalf("couldn't finalize statement: %s", err)
	}
	err = db.CreateScalarFunction("half", 1, nil, nil, nil)
	if err != nil {
		t.Errorf("couldn't destroy function: %s", err)
	}
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
		ctx.SetAuxData(0, re, nil)
	} else {
		//println("Reuse")
		var ok bool
		if re, ok = ad.(*regexp.Regexp); !ok {
			ctx.ResultError("AuxData not a regexp ")
			return
		}
	}
	m := re.MatchString(ctx.Text(1))
	ctx.ResultBool(m)
}

func TestRegexpFunction(t *testing.T) {
	db, err := Open("")
	if err != nil {
		t.Fatalf("couldn't open database file: %s", err)
	}
	defer db.Close()
	err = db.CreateScalarFunction("regexp", 2, nil, re, nil)
	if err != nil {
		t.Fatalf("couldn't create function: %s", err)
	}
	s, err := db.Prepare("select regexp('l.s[aeiouy]', name) from (select 'lisa' as name union all select 'bart' as name)")
	if err != nil {
		t.Fatalf("couldn't prepare statement: %s", err)
	}
	b, err := s.Next()
	if err != nil {
		t.Fatalf("couldn't step statement: %s", err)
	}
	if !b {
		t.Fatalf("No result")
	}
	i, _, err := s.ScanInt(0)
	if err != nil {
		t.Fatalf("couldn't scan result: %s", err)
	}
	if i != 1 {
		t.Errorf("Expected %d but got %d", 1, i)
	}
	b, err = s.Next()
	if err != nil {
		t.Fatalf("couldn't step statement: %s", err)
	}
	if !b {
		t.Fatalf("No result")
	}
	i, _, err = s.ScanInt(0)
	if err != nil {
		t.Fatalf("couldn't scan result: %s", err)
	}
	if i != 0 {
		t.Errorf("Expected %d but got %d", 0, i)
	}
	err = s.Finalize()
	if err != nil {
		t.Fatalf("couldn't finalize statement: %s", err)
	}
}
