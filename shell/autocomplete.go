// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package shell

import (
	"strings"

	"github.com/gwenn/gosqlite"
)

type pendingAction struct {
	action  sqlite.Action
	dbName  string
	tblName string
	typ     string
}

type CompletionCache struct {
	memDb          *sqlite.Conn    // SQLite FTS extension is used to do auto-completion
	insert         *sqlite.Stmt    // statement used to update col_names table
	pendingActions []pendingAction // actions trapped by the authorizer for deferred cache update
}

func CreateCache() (*CompletionCache, error) {
	db, err := sqlite.Open(":memory:")
	if err != nil {
		return nil, err
	}
	cc := &CompletionCache{memDb: db, pendingActions: make([]pendingAction, 0, 5)}
	if err = cc.init(); err != nil {
		db.Close()
		return nil, err
	}
	s, err := cc.memDb.Prepare("INSERT INTO col_names (db_name, tbl_name, type, col_name) VALUES (?, ?, ?, ?)")
	if err != nil {
		return nil, err
	}
	cc.insert = s
	return cc, nil
}

func (cc *CompletionCache) init() error {
	cmd := `CREATE VIRTUAL TABLE pragma_names USING fts4(name, args, tokenize=porter, matchinfo=fts3, notindexed=args);
	CREATE VIRTUAL TABLE func_names USING fts4(name, args, tokenize=porter, matchinfo=fts3, notindexed=args);
	CREATE VIRTUAL TABLE module_names USING fts4(name, args, tokenize=porter, matchinfo=fts3, notindexed=args);
	CREATE VIRTUAL TABLE cmd_names USING fts4(name, args, tokenize=porter, matchinfo=fts3, notindexed=args);
	CREATE VIRTUAL TABLE col_names USING fts4(db_name, tbl_name, type, col_name, tokenize=porter, matchinfo=fts3, notindexed=type);
	`
	var err error
	if err = cc.memDb.FastExec(cmd); err != nil {
		return err
	}
	if err = cc.memDb.Begin(); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			cc.memDb.Rollback()
		} else {
			err = cc.memDb.Commit()
		}
	}()
	s, err := cc.memDb.Prepare("INSERT INTO pragma_names (name, args) VALUES (?, ?)")
	if err != nil {
		return err
	}
	pragmas := []struct {
		Name string
		Args string
	}{
		{Name: "application_id", Args: "integer"},
		{Name: "auto_vacuum", Args: "0 | NONE | 1 | FULL | 2 | INCREMENTAL"},
		{Name: "automatic_index", Args: "boolean"},
		{Name: "busy_timeout", Args: "milliseconds"},
		{Name: "cache_size", Args: "pages or -kibibytes"},
		{Name: "cache_spill", Args: "boolean"},
		{Name: "case_sensitive_like=", Args: "boolean"}, // set-only
		{Name: "checkpoint_fullfsync", Args: "boolean"},
		{Name: "collation_list", Args: ""},  // no =
		{Name: "compile_options", Args: ""}, // no =
		//{Name: "count_changes", Args: "boolean"},
		//{Name: "data_store_directory", Args: "'directory-name'"},
		{Name: "database_list", Args: ""},
		//{Name: "default_cache_size", Args: "Number-of-pages"},
		{Name: "defer_foreign_keys", Args: "boolean"},
		//{Name: "empty_result_callbacks","boolean"},
		{Name: "encoding", Args: "UTF-8 | UTF-16 | UTF-16le | UTF-16be"},
		{Name: "foreign_key_check", Args: "(table-name)"}, // no =
		{Name: "foreign_key_list(", Args: "table-name"},   // no =
		{Name: "foreign_keys", Args: "boolean"},
		{Name: "freelist_count", Args: ""},
		//{Name: "full_column_names", Args: "boolean"},
		{Name: "fullfsync", Args: "boolean"},
		{Name: "ignore_check_constraints=", Args: "boolean"},
		{Name: "incremental_vacuum(", Args: "N"},
		{Name: "index_info(", Args: "index-name"}, // no =
		{Name: "index_list(", Args: "table-name"}, // no =
		{Name: "integrity_check", Args: "(N)"},
		{Name: "journal_mode", Args: "DELETE | TRUNCATE | PERSIST | MEMORY | WAL | OFF"},
		{Name: "journal_size_limit", Args: "N"},
		{Name: "legacy_file_format", Args: "boolean"},
		{Name: "locking_mode", Args: "NORMAL | EXCLUSIVE"},
		{Name: "max_page_count", Args: "N"},
		{Name: "mmap_size", Args: "N"},
		{Name: "page_count", Args: ""}, // no =
		{Name: "page_size", Args: "bytes"},
		//{Name: "parser_trace=", Args: "boolean"},
		{Name: "query_only", Args: "boolean"},
		{Name: "quick_check", Args: "(N)"}, // no =
		{Name: "read_uncommitted", Args: "boolean"},
		{Name: "recursive_triggers", Args: "boolean"},
		{Name: "reverse_unordered_selects", Args: "boolean"},
		{Name: "schema_version", Args: "integer"},
		{Name: "secure_delete", Args: "boolean"},
		//{Name: "short_column_names", Args: "boolean"},
		{Name: "shrink_memory", Args: ""}, // no =
		{Name: "soft_heap_limit", Args: "N"},
		//{Name: "stats", Args: ""},
		{Name: "synchronous", Args: "0 | OFF | 1 | NORMAL | 2 | FULL"},
		{Name: "table_info(", Args: "table-name"}, // no =
		{Name: "temp_store", Args: "0 | DEFAULT | 1 | FILE | 2 | MEMORY"},
		//{Name: "temp_store_directory", Args: "'directory-name'"},
		{Name: "user_version", Args: "integer"},
		//{Name: "vdbe_addoptrace=", Args: "boolean"},
		//{Name: "vdbe_debug=", Args: "boolean"},
		//{Name: "vdbe_listing=", Args: "boolean"},
		//{Name: "vdbe_trace=", Args: "boolean"},
		{Name: "wal_autocheckpoint", Args: "N"},
		{Name: "wal_checkpoint", Args: "(PASSIVE | FULL | RESTART)"}, // no =
		{Name: "writable_schema=", Args: "boolean"},                  // set-only
	}
	for _, pragma := range pragmas {
		if err = s.Exec(pragma.Name, pragma.Args); err != nil {
			return err
		}
	}
	if err = s.Finalize(); err != nil {
		return err
	}
	// Only built-in functions are supported.
	// TODO make possible to register extended/user-defined functions
	s, err = cc.memDb.Prepare("INSERT INTO func_names (name, args) VALUES (?, ?)")
	if err != nil {
		return err
	}
	funs := []struct {
		Name string
		Args string
	}{
		{Name: "abs(", Args: "X"},
		{Name: "changes()", Args: ""},
		{Name: "char(", Args: "X1,X2,...,XN"},
		{Name: "coalesce(", Args: "X,Y,..."},
		{Name: "glob(", Args: "X,Y"},
		{Name: "ifnull(", Args: "X,Y"},
		{Name: "instr(", Args: "X,Y"},
		{Name: "hex(", Args: "X"},
		{Name: "last_insert_rowid()", Args: ""},
		{Name: "length(", Args: "X"},
		{Name: "like(", Args: "X,Y[,Z]"},
		{Name: "likelihood(", Args: "X,Y"},
		{Name: "load_extension(", Args: "X[,Y]"},
		{Name: "lower(", Args: "X"},
		{Name: "ltrim(", Args: "X[,Y]"},
		{Name: "max(", Args: "X[,Y,...]"},
		{Name: "min(", Args: "X[,Y,...]"},
		{Name: "nullif(", Args: "X,Y"},
		{Name: "printf(", Args: "FORMAT,..."},
		{Name: "quote(", Args: "X"},
		{Name: "random()", Args: ""},
		{Name: "randomblob(", Args: "N"},
		{Name: "replace", Args: "X,Y,Z"},
		{Name: "round(", Args: "X[,Y]"},
		{Name: "rtrim(", Args: "X[,Y]"},
		{Name: "soundex(", Args: "X"},
		{Name: "sqlite_compileoption_get(", Args: "N"},
		{Name: "sqlite_compileoption_used(", Args: "X"},
		{Name: "sqlite_source_id()", Args: ""},
		{Name: "sqlite_version()", Args: ""},
		{Name: "substr(", Args: "X,Y[,Z]"},
		{Name: "total_changes()", Args: ""},
		{Name: "trim(", Args: "X[,Y]"},
		{Name: "typeof(", Args: "X"},
		{Name: "unlikely(", Args: "X"},
		{Name: "unicode(", Args: "X"},
		{Name: "upper(", Args: "X"},
		{Name: "zeroblob(", Args: "N"},
		// aggregate functions
		{Name: "avg(", Args: "X"},
		{Name: "count(", Args: "X|*"},
		{Name: "group_concat(", Args: "X[,Y]"},
		//{Name: "max(", Args: "X"},
		//{Name: "min(", Args: "X"},
		{Name: "sum(", Args: "X"},
		{Name: "total(", Args: "X"},
		// date functions
		{Name: "date(", Args: "timestring, modifier, modifier, ..."},
		{Name: "time(", Args: "timestring, modifier, modifier, ..."},
		{Name: "datetime(", Args: "timestring, modifier, modifier, ..."},
		{Name: "julianday(", Args: "timestring, modifier, modifier, ..."},
		{Name: "strftime(", Args: "format, timestring, modifier, modifier, ..."},
	}
	for _, fun := range funs {
		if err = s.Exec(fun.Name, fun.Args); err != nil {
			return err
		}
	}
	if err = s.Finalize(); err != nil {
		return err
	}
	// Only built-in modules are supported.
	// TODO make possible to register extended/user-defined modules
	s, err = cc.memDb.Prepare("INSERT INTO module_names (name, args) VALUES (?, ?)")
	if err != nil {
		return err
	}
	mods := []struct {
		Name string
		Args string
	}{
		{Name: "fts3(", Args: ""},
		{Name: "fts4(", Args: ""},
		{Name: "rtree(", Args: ""},
	}
	for _, mod := range mods {
		if err = s.Exec(mod.Name, mod.Args); err != nil {
			return err
		}
	}
	if err = s.Finalize(); err != nil {
		return err
	}
	s, err = cc.memDb.Prepare("INSERT INTO cmd_names (name, args) VALUES (?, ?)")
	if err != nil {
		return err
	}
	cmds := []struct {
		Name string
		Args string
	}{
		{Name: ".backup", Args: "?DB? FILE"},
		{Name: ".bail", Args: "ON|OFF"},
		{Name: ".clone", Args: "NEWDB"},
		{Name: ".databases", Args: ""},
		{Name: ".dump", Args: "?TABLE? ..."},
		{Name: ".echo", Args: "ON|OFF"},
		{Name: ".eqp", Args: "ON|OFF"},
		{Name: ".exit", Args: ""},
		{Name: ".explain", Args: "?ON|OFF?"},
		{Name: ".fullschema", Args: ""},
		//{Name: ".header", Args: "ON|OFF"},
		{Name: ".headers", Args: "ON|OFF"},
		{Name: ".help", Args: ""},
		{Name: ".import", Args: "FILE TABLE"},
		{Name: ".indices", Args: "?TABLE?"},
		{Name: ".load", Args: "FILE ?ENTRY?"},
		{Name: ".log", Args: "FILE|off"},
		{Name: ".mode", Args: "MODE ?TABLE?"},
		{Name: ".nullvalue", Args: "STRING"},
		{Name: ".open", Args: "?FILENAME?"},
		{Name: ".output", Args: "stdout | FILENAME"},
		{Name: ".print", Args: "STRING..."},
		{Name: ".prompt", Args: "MAIN CONTINUE"},
		{Name: ".quit", Args: ""},
		{Name: ".read", Args: "FILENAME"},
		{Name: ".restore", Args: "?DB? FILE"},
		{Name: ".save", Args: "FILE"},
		{Name: ".schema", Args: "?TABLE?"},
		{Name: ".separator", Args: "STRING ?NL?"},
		{Name: ".shell", Args: "CMD ARGS..."},
		{Name: ".show", Args: ""},
		{Name: ".stats", Args: "ON|OFF"},
		{Name: ".system", Args: "CMD ARGS..."},
		{Name: ".tables", Args: "?TABLE?"},
		{Name: ".timeout", Args: "MS"},
		{Name: ".timer", Args: "ON|OFF"},
		{Name: ".trace", Args: "FILE|off"},
		{Name: ".vfsname", Args: "?AUX?"},
		{Name: ".width", Args: "NUM1 NUM2 ..."},
	}
	for _, cmd := range cmds {
		if err = s.Exec(cmd.Name, cmd.Args); err != nil {
			return err
		}
	}
	if err = s.Finalize(); err != nil {
		return err
	}
	return err
}

func (cc *CompletionCache) Close() error {
	if err := cc.insert.Finalize(); err != nil {
		return err
	}
	return cc.memDb.Close()
}

func (cc *CompletionCache) Cache(db *sqlite.Conn) error {
	// It seems not necessary to disable the SQLite statement cache to make this authorizer work
	// because DDL are re-prepared when the schema has been touched.
	db.SetAuthorizer(func(udp interface{}, action sqlite.Action, arg1, arg2, dbName, triggerName string) sqlite.Auth {
		switch action {
		case sqlite.Detach:
			cc.pendingActions = append(cc.pendingActions, pendingAction{action: action, dbName: arg1})
		case sqlite.Attach:
			// database name is not available, only the path...
			if arg1 != "" && arg1 != ":memory:" { // temporary db: "" and memory db: ":memory:" are empty when attached.
				cc.pendingActions = append(cc.pendingActions, pendingAction{action: action, dbName: arg1})
			}
		case sqlite.DropTable, sqlite.DropTempTable, sqlite.DropView, sqlite.DropTempView, sqlite.DropVTable:
			cc.pendingActions = append(cc.pendingActions, pendingAction{action: action, dbName: dbName, tblName: arg1})
		case sqlite.CreateTable, sqlite.CreateTempTable, sqlite.CreateVTable:
			cc.pendingActions = append(cc.pendingActions, pendingAction{action: action, dbName: dbName, tblName: arg1, typ: "table"})
		case sqlite.CreateView, sqlite.CreateTempView:
			cc.pendingActions = append(cc.pendingActions, pendingAction{action: action, dbName: dbName, tblName: arg1, typ: "view"})
		case sqlite.AlterTable:
			cc.pendingActions = append(cc.pendingActions, pendingAction{action: action, dbName: arg1, tblName: arg2, typ: "table"})
			// TODO trigger, index
		}
		return sqlite.AuthOk
	}, nil)
	dbNames, err := db.Databases()
	if err != nil {
		return err
	}
	// cache databases schema
	for dbName := range dbNames {
		err = cc.cache(db, dbName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cc *CompletionCache) cache(db *sqlite.Conn, dbName string) error {
	ts, err := db.Tables(dbName)
	if err != nil {
		return err
	}
	if dbName == "temp" {
		ts = append(ts, "sqlite_temp_master")
	} else {
		ts = append(ts, "sqlite_master")
	}

	for _, table := range ts {
		if err = cc.cacheTable(db, dbName, table, "table"); err != nil {
			return err
		}
	}

	vs, err := db.Views(dbName)
	if err != nil {
		return err
	}
	for _, view := range vs {
		if err = cc.cacheTable(db, dbName, view, "view"); err != nil {
			return err
		}
	}

	return nil
}

func (cc *CompletionCache) cacheTable(db *sqlite.Conn, dbName, tblName, typ string) error {
	cs, err := db.Columns(dbName, tblName)
	if err != nil {
		return err
	}
	for _, c := range cs {
		if err = cc.insert.Exec(dbName, tblName, typ, c.Name); err != nil {
			return err
		}
	}
	return nil
}

func (cc *CompletionCache) Update(db *sqlite.Conn) error {
	for _, pa := range cc.pendingActions {
		switch pa.action {
		case sqlite.Attach:
			dbs, err := db.Databases()
			if err != nil {
				return err
			}
			for dbName, dbPath := range dbs {
				if dbPath == pa.dbName {
					if err := cc.cache(db, dbName); err != nil {
						return err
					}
				}
			}
		case sqlite.Detach:
			if err := cc.memDb.Exec("DELETE FROM col_names WHERE db_name = ?", pa.dbName); err != nil {
				return err
			}
		case sqlite.AlterTable:
			if err := cc.memDb.Exec("DELETE FROM col_names WHERE db_name = ? AND tbl_name = ?", pa.dbName, pa.tblName); err != nil {
				return err
			}
			fallthrough
		case sqlite.CreateTable, sqlite.CreateTempTable, sqlite.CreateView, sqlite.CreateTempView, sqlite.CreateVTable:
			if err := cc.cacheTable(db, pa.dbName, pa.tblName, pa.typ); err != nil {
				return err
			}
		case sqlite.DropTable, sqlite.DropTempTable, sqlite.DropView, sqlite.DropTempView, sqlite.DropVTable:
			if err := cc.memDb.Exec("DELETE FROM col_names WHERE db_name = ? AND tbl_name = ?", pa.dbName, pa.tblName); err != nil {
				return err
			}
		}
	}
	cc.pendingActions = cc.pendingActions[:0]
	return nil
}

func (cc *CompletionCache) Flush(db *sqlite.Conn) error {
	cc.pendingActions = cc.pendingActions[:0]
	return cc.memDb.FastExec("DELETE FROM col_names")
}

func (cc *CompletionCache) CompletePragma(prefix string) ([]string, error) {
	return cc.complete("pragma_names", prefix)
}
func (cc *CompletionCache) CompleteFunc(prefix string) ([]string, error) {
	return cc.complete("func_names", prefix)
}
func (cc *CompletionCache) CompleteCmd(prefix string) ([]string, error) {
	return cc.complete("cmd_names", prefix)
}

func (cc *CompletionCache) complete(tbl, prefix string) ([]string, error) {
	s, err := cc.memDb.Prepare("SELECT name FROM "+tbl+" WHERE name MATCH ?||'*' ORDER BY 1", prefix)
	if err != nil {
		return nil, err
	}
	defer s.Finalize()
	var names []string
	if err = s.Select(func(s *sqlite.Stmt) error {
		name, _ := s.ScanText(0)
		names = append(names, name)
		return nil
	}); err != nil {
		return nil, err
	}
	return names, nil
}

func (cc *CompletionCache) CompleteDbName(prefix string) ([]string, error) {
	s, err := cc.memDb.Prepare("SELECT DISTINCT db_name FROM col_names WHERE db_name MATCH ?||'*' ORDER BY 1", prefix)
	if err != nil {
		return nil, err
	}
	defer s.Finalize()
	var names []string
	if err = s.Select(func(s *sqlite.Stmt) error {
		name, _ := s.ScanText(0)
		names = append(names, name)
		return nil
	}); err != nil {
		return nil, err
	}
	return names, nil
}

func (cc *CompletionCache) CompleteTableName(dbName, prefix, typ string) ([]string, error) {
	args := make([]interface{}, 0, 3)
	if dbName != "" {
		args = append(args, dbName)
	}
	args = append(args, prefix)
	if typ != "" {
		args = append(args, typ)
	}
	var sql string
	if dbName == "" {
		if typ == "" {
			sql = "SELECT DISTINCT tbl_name FROM col_names WHERE tbl_name MATCH ?||'*' ORDER BY 1"
		} else {
			sql = "SELECT DISTINCT tbl_name FROM col_names WHERE tbl_name MATCH ?||'*' AND type = ? ORDER BY 1"
		}
	} else {
		if typ == "" {
			sql = "SELECT DISTINCT tbl_name FROM col_names WHERE db_name = ? AND tbl_name MATCH ?||'*' ORDER BY 1"
		} else {
			sql = "SELECT DISTINCT tbl_name FROM col_names WHERE db_name = ? AND tbl_name MATCH ?||'*' AND type = ? ORDER BY 1"
		}
	}
	s, err := cc.memDb.Prepare(sql, args...)
	if err != nil {
		return nil, err
	}
	defer s.Finalize()
	var names []string
	if err = s.Select(func(s *sqlite.Stmt) error {
		name, _ := s.ScanText(0)
		names = append(names, name)
		return nil
	}); err != nil {
		return nil, err
	}
	return names, nil
}

// tbl_names is mandatory
func (cc *CompletionCache) CompleteColName(dbName string, tbl_names []string, prefix string) ([]string, error) {
	args := make([]interface{}, 0, 10)
	if dbName != "" {
		args = append(args, dbName)
	}
	phs := make([]string, 0, 10)
	for _, tbl_name := range tbl_names {
		args = append(args, tbl_name)
		phs = append(phs, "?")
	}
	args = append(args, prefix)
	var sql string
	if dbName == "" {
		sql = "SELECT DISTINCT col_name FROM col_names WHERE tbl_name IN (" + strings.Join(phs, ",") + ") AND col_name MATCH ?||'*' ORDER BY 1"
	} else {
		sql = "SELECT DISTINCT col_name FROM col_names WHERE db_name = ? AND tbl_name IN (" + strings.Join(phs, ",") + ") AND col_name MATCH ?||'*' ORDER BY 1"
	}
	s, err := cc.memDb.Prepare(sql, args...)
	if err != nil {
		return nil, err
	}
	defer s.Finalize()
	var names []string
	if err = s.Select(func(s *sqlite.Stmt) error {
		name, _ := s.ScanText(0)
		names = append(names, name)
		return nil
	}); err != nil {
		return nil, err
	}
	return names, nil
}
