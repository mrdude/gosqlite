package sqlite_test

import (
	"fmt"
	. "github.com/gwenn/gosqlite"
	"testing"
)

func trace(d interface{}, t string) {
	fmt.Printf("%s: %s\n", d, t)
}

func authorizer(d interface{}, action Action, arg1, arg2, arg3, arg4 string) Auth {
	fmt.Printf("%s: %d, %s, %s, %s, %s\n", d, action, arg1, arg2, arg3, arg4)
	return AUTH_OK
}

func profile(d interface{}, sql string, nanoseconds uint64) {
	fmt.Printf("%s: %s = %d\n", d, sql, nanoseconds/1000)
}

func progressHandler(d interface{}) int {
	fmt.Print("+")
	return 0
}

func TestNoTrace(t *testing.T) {
	db, err := Open("")
	if err != nil {
		t.Fatalf("couldn't open database file: %s", err)
	}
	db.Trace(nil, nil)
	db.SetAuthorizer(nil, nil)
	db.Profile(nil, nil)
	db.ProgressHandler(nil, 0, nil)
	db.BusyHandler(nil, nil)
	db.Close()
}

func TestTrace(t *testing.T) {
	db, err := Open("")
	db.Trace(trace, "TRACE")
	err = db.SetAuthorizer(authorizer, "AUTH")
	if err != nil {
		t.Fatal("couldn't set an authorizer", err)
	}
	db.Profile(profile, "PROFILE")
	db.ProgressHandler(progressHandler, 1, /*20*/ nil)
	db.Exists("SELECT 1 WHERE 1 = ?", 1)
}
