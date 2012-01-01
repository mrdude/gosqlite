package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"testing"
)

func half(ctx *Context, nArg int) {
	ctx.ResultDouble(ctx.Double(0) / 2)
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