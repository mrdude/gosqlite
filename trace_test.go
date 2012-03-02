package sqlite_test

import (
	"fmt"
	. "github.com/gwenn/gosqlite"
	"testing"
)

func init() {
	//ConfigLog(log, "LOG")
}

func trace(d interface{}, sql string) {
	//fmt.Printf("%s: %s\n", d, sql)
}

func authorizer(d interface{}, action Action, arg1, arg2, dbName, triggerName string) Auth {
	//fmt.Printf("%s: %d, %s, %s, %s, %s\n", d, action, arg1, arg2, dbName, triggerName)
	return AUTH_OK
}

func profile(d interface{}, sql string, nanoseconds uint64) {
	//fmt.Printf("%s: %s = %d µs\n", d, sql, nanoseconds/1e3)
}

func progressHandler(d interface{}) bool {
	//fmt.Print("+")
	return false
}

func commitHook(d interface{}) int {
	fmt.Printf("%s\n", d)
	return 0
}

func rollbackHook(d interface{}) {
	fmt.Printf("%s\n", d)
}

func updateHook(d interface{}, a Action, dbName, tableName string, rowId int64) {
	fmt.Printf("%s: %d, %s.%s.%d\n", d, a, dbName, tableName, rowId)
}

func log(d interface{}, err error, msg string) {
	fmt.Printf("%s: %s, %s\n", d, err, msg)
}

func TestNoTrace(t *testing.T) {
	db, err := Open("")
	checkNoError(t, err, "couldn't open database: %s")
	defer db.Close()
	db.Trace(nil, nil)
	db.SetAuthorizer(nil, nil)
	db.Profile(nil, nil)
	db.ProgressHandler(nil, 0, nil)
	db.BusyHandler(nil, nil)
	db.CommitHook(nil, nil)
	db.RollbackHook(nil, nil)
	db.UpdateHook(nil, nil)
}

func TestTrace(t *testing.T) {
	db, err := Open("")
	checkNoError(t, err, "couldn't open database: %s")
	defer db.Close()
	db.Trace(trace, "TRACE")
	err = db.SetAuthorizer(authorizer, "AUTH")
	checkNoError(t, err, "couldn't set an authorizer")
	db.Profile(profile, "PROFILE")
	db.ProgressHandler(progressHandler, 1, nil /*20*/)
	db.CommitHook(commitHook, "CMT")
	db.RollbackHook(rollbackHook, "RBK")
	db.UpdateHook(updateHook, "UPD")
	db.Exists("SELECT 1 WHERE 1 = ?", 1)
}

func TestLog(t *testing.T) {
	Log(0, "One message")
}
