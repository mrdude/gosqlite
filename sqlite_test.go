package sqlite

import (
	"fmt"
	"strings"
	"testing"
	"os"
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

func open(t *testing.T) *Conn {
	db, err := Open("", OPEN_READWRITE, OPEN_CREATE, OPEN_FULLMUTEX, OPEN_URI)
	if err != nil {
		t.Fatalf("couldn't open database file: %s", err)
	}
	if db == nil {
		t.Fatal("opened database is nil")
	}
	//db.Trace(trace, "TRACE")
	/*
		err = db.SetAuthorizer(authorizer, "AUTH")
		if err != nil {
			t.Fatal("couldn't set an authorizer", err)
		}
	*/
	//db.Profile(profile, "PROFILE")
	return db
}

func createTable(db *Conn, t *testing.T) {
	err := db.Exec("DROP TABLE IF EXISTS test;" +
		"CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT," +
		" float_num REAL, int_num INTEGER, a_string TEXT); -- bim")
	if err != nil {
		t.Fatalf("error creating table: %s", err)
	}
}

func TestVersion(t *testing.T) {
	v := Version()
	if !strings.HasPrefix(v, "3") {
		t.Fatalf("unexpected library version: %s", v)
	}
}

func TestOpen(t *testing.T) {
	db := open(t)
	db.Trace(nil, nil)
	db.SetAuthorizer(nil, nil)
	db.Close()
}

func TestEnableFKey(t *testing.T) {
	db := open(t)
	defer db.Close()
	b, err := db.IsFKeyEnabled()
	if err != nil {
		t.Fatalf("Error while checking if FK are enabled: %s", err)
	}
	if !b {
		b, err = db.EnableFKey(true)
		if err != nil {
			t.Fatalf("Error while enabling FK: %s", err)
		}
		if !b {
			t.Error("cannot enabled FK")
		}
	}
}

func TestCreateTable(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)
}

func TestExists(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)
	b, err := db.Exists("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	if b {
		t.Error("No row expected")
	}
	b, err = db.Exists("SELECT count(1) FROM test")
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	if !b {
		t.Error("One row expected")
	}
}

func TestInsert(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)
	for i := 0; i < 1000; i++ {
		ierr := db.Exec("INSERT INTO test (float_num, int_num, a_string) VALUES (?, ?, ?)", float64(i)*float64(3.14), i, "hello")
		if ierr != nil {
			t.Fatalf("insert error: %s", ierr)
		}
		c := db.Changes()
		if c != 1 {
			t.Errorf("insert error: %d <> 1", c)
		}
	}

	lastId := db.LastInsertRowid()
	if lastId != 1000 {
		t.Errorf("last insert row id error: %d <> 1000", lastId)
	}

	cs, _ := db.Prepare("SELECT COUNT(*) FROM test")
	defer cs.Finalize()

	paramCount := cs.BindParameterCount()
	if paramCount != 0 {
		t.Errorf("bind parameter count error: %d <> 0", paramCount)
	}
	columnCount := cs.ColumnCount()
	if columnCount != 1 {
		t.Errorf("column count error: %d <> 1", columnCount)
	}

	if ok, err := cs.Next(); !ok {
		if err != nil {
			t.Fatalf("error preparing count: %s", err)
		}
		t.Fatal("no result for count")
	}
	var i int
	err := cs.Scan(&i)
	if err != nil {
		t.Fatalf("error scanning count: %s", err)
	}
	if i != 1000 {
		t.Errorf("count should be 1000, but it is %d", i)
	}
}

func TestInsertWithStatement(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)
	s, serr := db.Prepare("INSERT INTO test (float_num, int_num, a_string) VALUES (?, ?, ?)")
	if serr != nil {
		t.Fatalf("prepare error: %s", serr)
	}
	if s == nil {
		t.Fatal("statement is nil")
	}
	defer s.Finalize()

	paramCount := s.BindParameterCount()
	if paramCount != 3 {
		t.Errorf("bind parameter count error: %d <> 3", paramCount)
	}

	for i := 0; i < 1000; i++ {
		ierr := s.Exec(float64(i)*float64(3.14), i, "hello")
		if ierr != nil {
			t.Fatalf("insert error: %s", ierr)
		}
		c := db.Changes()
		if c != 1 {
			t.Errorf("insert error: %d <> 1", c)
		}
	}
	s.Finalize()

	cs, _ := db.Prepare("SELECT COUNT(*) FROM test")
	defer cs.Finalize()
	if ok, _ := cs.Next(); !ok {
		t.Fatal("no result for count")
	}
	var i int
	err := cs.Scan(&i)
	if err != nil {
		t.Fatalf("error scanning count: %s", err)
	}
	if i != 1000 {
		t.Errorf("count should be 1000, but it is %d", i)
	}

	rs, _ := db.Prepare("SELECT float_num, int_num, a_string FROM test where a_string like ? ORDER BY int_num LIMIT 2", "hel%")
	columnCount := rs.ColumnCount()
	if columnCount != 3 {
		t.Errorf("column count error: %d <> 3", columnCount)
	}
	secondColumnName := rs.ColumnName(1)
	if secondColumnName != "int_num" {
		t.Errorf("column name error: %s <> 'int_num'", secondColumnName)
	}

	var fnum float64
	var inum int64
	var sstr string
	if ok, _ := rs.Next(); ok {
		rs.Scan(&fnum, &inum, &sstr)
		if fnum != 0 {
			t.Errorf("Expected 0 <> %f\n", fnum)
		}
		if inum != 0 {
			t.Errorf("Expected 0 <> %d\n", inum)
		}
		if sstr != "hello" {
			t.Errorf("Expected 'hello' <> %s\n", sstr)
		}
	}
	if ok, _ := rs.Next(); ok {
		var fnum float64
		var inum int64
		var sstr string
		rs.NamedScan("a_string", &sstr, "float_num", &fnum, "int_num", &inum)
		if fnum != 3.14 {
			t.Errorf("Expected 3.14 <> %f\n", fnum)
		}
		if inum != 1 {
			t.Errorf("Expected 1 <> %d\n", inum)
		}
		if sstr != "hello" {
			t.Errorf("Expected 'hello' <> %s\n", sstr)
		}
	}
}

func TestTables(t *testing.T) {
	db := open(t)
	defer db.Close()

	tables, err := db.Tables()
	if err != nil {
		t.Fatalf("error looking for tables: %s", err)
	}
	if len(tables) != 0 {
		t.Errorf("Expected no table but got %d\n", len(tables))
	}
	createTable(db, t)
	tables, err = db.Tables()
	if err != nil {
		t.Fatalf("error looking for tables: %s", err)
	}
	if len(tables) != 1 {
		t.Errorf("Expected one table but got %d\n", len(tables))
	}
	if tables[0] != "test" {
		t.Errorf("Wrong table name: 'test' <> %s\n", tables[0])
	}
}

func TestColumns(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)

	columns, err := db.Columns("test")
	if err != nil {
		t.Fatalf("error listing columns: %s", err)
	}
	if len(columns) != 4 {
		t.Fatalf("Expected 4 columns <> %d", len(columns))
	}
	column := columns[2]
	if column.Name != "int_num" {
		t.Errorf("Wrong column name: 'int_num' <> %s", column.Name)
	}
}

func TestForeignKeys(t *testing.T) {
	db := open(t)
	defer db.Close()

	err := db.Exec("CREATE TABLE parent (id INTEGER PRIMARY KEY);" +
		"CREATE TABLE child (id INTEGER PRIMARY KEY, parentId INTEGER, " +
		"FOREIGN KEY (parentId) REFERENCES parent(id));")
	if err != nil {
		t.Fatalf("error creating tables: %s", err)
	}
	fks, err := db.ForeignKeys("child")
	if err != nil {
		t.Fatalf("error listing FKs: %s", err)
	}
	if len(fks) != 1 {
		t.Fatalf("Expected 1 FK <> %d", len(fks))
	}
	fk := fks[0]
	if fk.From[0] != "parentId" || fk.Table != "parent" || fk.To[0] != "id" {
		t.Errorf("Unexpected FK data: %#v", fk)
	}
}

func TestBlob(t *testing.T) {
	db := open(t)
	defer db.Close()

	err := db.Exec("CREATE TABLE test (content BLOB);")
	if err != nil {
		t.Fatalf("error creating tables: %s", err)
	}
	s, err := db.Prepare("INSERT INTO test VALUES (?)")
	if err != nil {
		t.Fatalf("prepare error: %s", err)
	}
	if s == nil {
		t.Fatal("statement is nil")
	}
	defer s.Finalize()
	err = s.Exec(ZeroBlobLength(10))
	if err != nil {
		t.Fatalf("insert error: %s", err)
	}
	rowid := db.LastInsertRowid()

	bw, err := db.NewBlobReadWriter("main", "test", "content", rowid)
	if err != nil {
		t.Fatalf("blob open error: %s", err)
	}
	defer bw.Close()
	content := []byte("Clob")
	n, err := bw.Write(content)
	if err != nil {
		t.Fatalf("blob write error: %s", err)
	}

	br, err := db.NewBlobReader("main", "test", "content", rowid)
	if err != nil {
		t.Fatalf("blob open error: %s", err)
	}
	defer br.Close()
	size, err := br.Size()
	if err != nil {
		t.Fatalf("blob size error: %s", err)
	}
	content = make([]byte, size)
	n, err = br.Read(content)
	if err != nil {
		t.Fatalf("blob read error: %s", err)
	}
	if n != 10 {
		t.Fatalf("Expected 10 bytes <> %d", n)
	}
	//fmt.Printf("%#v\n", content)
	br.Close()
}

func BenchmarkScan(b *testing.B) {
	b.StopTimer()
	db, _ := Open("")
	defer db.Close()
	db.Exec("DROP TABLE IF EXISTS test")
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, float_num REAL, int_num INTEGER, a_string TEXT)")
	s, _ := db.Prepare("INSERT INTO test (float_num, int_num, a_string) VALUES (?, ?, ?)")

	for i := 0; i < 1000; i++ {
		s.Exec(float64(i)*float64(3.14), i, "hello")
	}
	s.Finalize()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cs, _ := db.Prepare("SELECT float_num, int_num, a_string FROM test")

		var fnum float64
		var inum int64
		var sstr string

		var ok bool
		var err os.Error
		for ok, err = cs.Next(); ok; ok, err = cs.Next() {
			cs.Scan(&fnum, &inum, &sstr)
		}
		if err != nil {
			panic(err)
		}
		cs.Finalize()
	}
}

func BenchmarkNamedScan(b *testing.B) {
	b.StopTimer()
	db, _ := Open("")
	defer db.Close()
	db.Exec("DROP TABLE IF EXISTS test")
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, float_num REAL, int_num INTEGER, a_string TEXT)")
	s, _ := db.Prepare("INSERT INTO test (float_num, int_num, a_string) VALUES (?, ?, ?)")

	for i := 0; i < 1000; i++ {
		s.Exec(float64(i)*float64(3.14), i, "hello")
	}
	s.Finalize()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cs, _ := db.Prepare("SELECT float_num, int_num, a_string FROM test")

		var fnum float64
		var inum int64
		var sstr string

		var ok bool
		var err os.Error
		for ok, err = cs.Next(); ok; ok, err = cs.Next() {
			cs.NamedScan("float_num", &fnum, "int_num", &inum, "a_string", &sstr)
		}
		if err != nil {
			panic(err)
		}
		cs.Finalize()
	}
}

func BenchmarkInsert(b *testing.B) {
	db, _ := Open("")
	defer db.Close()
	db.Exec("DROP TABLE IF EXISTS test")
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT," +
		" float_num REAL, int_num INTEGER, a_string TEXT)")
	s, _ := db.Prepare("INSERT INTO test (float_num, int_num, a_string)" +
		" VALUES (?, ?, ?)")
	defer s.Finalize()

	for i := 0; i < b.N; i++ {
		s.Exec(float64(i)*float64(3.14), i, "hello")
	}
}
