// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package shell

import (
	"strings"

	"github.com/gwenn/gosqlite"
)

type CompletionCache struct {
	memDb    *sqlite.Conn
	dbNames  []string // "main", "temp", ...
	dbCaches map[string]*databaseCache
}

type databaseCache struct {
	schemaVersion int               //
	tableNames    map[string]string // lowercase name => original name
	viewNames     map[string]string
	columnNames   map[string][]string // lowercase table name => column name
	// idxNames  []string // indexed by dbName (seems useful only in DROP INDEX statement)
	// trigNames []string // trigger by dbName (seems useful only in DROP TRIGGER statement)
}

func CreateCache() (*CompletionCache, error) {
	db, err := sqlite.Open(":memory:")
	if err != nil {
		return nil, err
	}
	cc := &CompletionCache{memDb: db, dbNames: make([]string, 0, 2), dbCaches: make(map[string]*databaseCache)}
	if err = cc.init(); err != nil {
		db.Close()
		return nil, err
	}
	return cc, nil
}

func (cc *CompletionCache) init() error {
	cmd := `CREATE VIRTUAL TABLE pragmaNames USING fts4(name, args, tokenize=porter, matchinfo=fts3, notindexed="args");
	CREATE VIRTUAL TABLE funcNames USING fts4(name, args, tokenize=porter, matchinfo=fts3, notindexed="args");
	CREATE VIRTUAL TABLE moduleNames USING fts4(name, args, tokenize=porter, matchinfo=fts3, notindexed="args");
	CREATE VIRTUAL TABLE cmdNames USING fts4(name, args, tokenize=porter, matchinfo=fts3, notindexed="args");
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
	s, err := cc.memDb.Prepare("INSERT INTO pragmaNames (name, args) VALUES (?, ?)")
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
	s, err = cc.memDb.Prepare("INSERT INTO funcNames (name, args) VALUES (?, ?)")
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
	s, err = cc.memDb.Prepare("INSERT INTO moduleNames (name, args) VALUES (?, ?)")
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
	s, err = cc.memDb.Prepare("INSERT INTO cmdNames (name, args) VALUES (?, ?)")
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
		{Name: ".exit", Args: ""},
		{Name: ".explain", Args: "?ON|OFF?"},
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
		{Name: ".separator", Args: "STRING"},
		{Name: ".show", Args: ""},
		{Name: ".stats", Args: "ON|OFF"},
		{Name: ".tables", Args: "?TABLE?"},
		{Name: ".timeout", Args: "MS"},
		{Name: ".trace", Args: "FILE|off"},
		{Name: ".vfsname", Args: "?AUX?"},
		{Name: ".width", Args: "NUM1 NUM2 ..."},
		{Name: ".timer", Args: "ON|OFF"},
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
	return cc.memDb.Close()
}

func (cc *CompletionCache) Update(db *sqlite.Conn) error {
	// update database list (TODO only on ATTACH ...)
	cc.dbNames = cc.dbNames[:0]
	dbNames, err := db.Databases()
	if err != nil {
		return err
	}
	// update databases cache
	for dbName := range dbNames {
		cc.dbNames = append(cc.dbNames, dbName)
		dbc := cc.dbCaches[dbName]
		if dbc == nil {
			dbc = &databaseCache{schemaVersion: -1, tableNames: make(map[string]string), viewNames: make(map[string]string), columnNames: make(map[string][]string)}
			cc.dbCaches[dbName] = dbc
		}
		err = dbc.update(db, dbName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dc *databaseCache) update(db *sqlite.Conn, dbName string) error {
	var sv int
	if sv, err := db.SchemaVersion(dbName); err != nil {
		return err
	} else if sv == dc.schemaVersion { // up to date
		return nil
	}

	ts, err := db.Tables(dbName)
	if err != nil {
		return err
	}
	if dbName == "temp" {
		ts = append(ts, "sqlite_temp_master")
	} else {
		ts = append(ts, "sqlite_master")
	}
	// clear
	for table := range dc.tableNames {
		delete(dc.tableNames, table)
	}
	for _, table := range ts {
		dc.tableNames[strings.ToLower(table)] = table // TODO unicode
	}

	vs, err := db.Views(dbName)
	if err != nil {
		return err
	}
	// clear
	for view := range dc.viewNames {
		delete(dc.viewNames, view)
	}
	for _, view := range vs {
		dc.viewNames[strings.ToLower(view)] = view // TODO unicode
	}

	// drop
	for table := range dc.columnNames {
		if _, ok := dc.tableNames[table]; ok {
			continue
		} else if _, ok := dc.viewNames[table]; ok {
			continue
		}
		delete(dc.columnNames, table)
	}

	for table := range dc.tableNames {
		cs, err := db.Columns(dbName, table)
		if err != nil {
			return err
		}
		columnNames := dc.columnNames[table]
		columnNames = columnNames[:0]
		for _, c := range cs {
			columnNames = append(columnNames, c.Name)
		}
		dc.columnNames[table] = columnNames
	}
	for view := range dc.viewNames {
		cs, err := db.Columns(dbName, view)
		if err != nil {
			return err
		}
		columnNames := dc.columnNames[view]
		columnNames = columnNames[:0]
		for _, c := range cs {
			columnNames = append(columnNames, c.Name)
		}
		dc.columnNames[view] = columnNames
	}

	dc.schemaVersion = sv
	return nil
}

func (cc *CompletionCache) CompletePragma(prefix string) ([]string, error) {
	return cc.complete("pragmaNames", prefix)
}
func (cc *CompletionCache) CompleteFunc(prefix string) ([]string, error) {
	return cc.complete("funcNames", prefix)
}
func (cc *CompletionCache) CompleteCmd(prefix string) ([]string, error) {
	return cc.complete("cmdNames", prefix)
}

func (cc *CompletionCache) complete(tbl, prefix string) ([]string, error) {
	s, err := cc.memDb.Prepare("SELECT name FROM " + tbl + " WHERE name MATCH ?||'*' ORDER BY 1")
	if err != nil {
		return nil, err
	}
	defer s.Finalize()
	var names []string
	if err = s.Select(func(s *sqlite.Stmt) error {
		name, _ := s.ScanText(0)
		names = append(names, name)
		return nil
	}, prefix); err != nil {
		return nil, err
	}
	return names, nil
}
