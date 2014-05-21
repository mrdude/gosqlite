// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build all

package sqlite

import (
	"sort"

	"github.com/sauerbraten/radix"
)

type completionCache struct {
	dbNames []string // "main", "temp", ...
}

type databaseCache struct {
	schemaVersion int //
	tableNames    []string
	viewNames     []string
	columnNames   map[string][]string
	// idxNames  []string // indexed by dbName (seems useful only in DROP INDEX statement)
	// trigNames []string // trigger by dbName (seems useful only in DROP TRIGGER statement)
}

var pragmaNames = radix.New()

// Only built-in functions are supported.
// TODO make possible to register extended/user-defined functions
var funcNames = radix.New()

// Only built-in modules are supported.
// TODO make possible to register extended/user-defined modules
var moduleNames = radix.New()

func init() {
	radixSet(pragmaNames, "application_id", "integer")
	radixSet(pragmaNames, "auto_vacuum", "0 | NONE | 1 | FULL | 2 | INCREMENTAL")
	radixSet(pragmaNames, "automatic_index", "boolean")
	radixSet(pragmaNames, "busy_timeout", "milliseconds")
	radixSet(pragmaNames, "cache_size", "pages or -kibibytes")
	radixSet(pragmaNames, "cache_spill", "boolean")
	radixSet(pragmaNames, "case_sensitive_like=", "boolean") // set-only
	radixSet(pragmaNames, "checkpoint_fullfsync", "boolean")
	radixSet(pragmaNames, "collation_list", "")  // no =
	radixSet(pragmaNames, "compile_options", "") // no =
	//radixSet(pragmaNames,"count_changes", "boolean")
	//radixSet(pragmaNames,"data_store_directory", "'directory-name'")
	radixSet(pragmaNames, "database_list", "")
	//radixSet(pragmaNames,"default_cache_size", "Number-of-pages")
	radixSet(pragmaNames, "defer_foreign_keys", "boolean")
	//radixSet(pragmaNames,"empty_result_callbacks","boolean")
	radixSet(pragmaNames, "encoding", "UTF-8 | UTF-16 | UTF-16le | UTF-16be")
	radixSet(pragmaNames, "foreign_key_check", "(table-name)") // no =
	radixSet(pragmaNames, "foreign_key_list(", "table-name")   // no =
	radixSet(pragmaNames, "foreign_keys", "boolean")
	radixSet(pragmaNames, "freelist_count", "")
	//radixSet(pragmaNames,"full_column_names", "boolean")
	radixSet(pragmaNames, "fullfsync", "boolean")
	radixSet(pragmaNames, "ignore_check_constraints=", "boolean")
	radixSet(pragmaNames, "incremental_vacuum(", "N")
	radixSet(pragmaNames, "index_info(", "index-name") // no =
	radixSet(pragmaNames, "index_list(", "table-name") // no =
	radixSet(pragmaNames, "integrity_check", "(N)")
	radixSet(pragmaNames, "journal_mode", "DELETE | TRUNCATE | PERSIST | MEMORY | WAL | OFF")
	radixSet(pragmaNames, "journal_size_limit", "N")
	radixSet(pragmaNames, "legacy_file_format", "boolean")
	radixSet(pragmaNames, "locking_mode", "NORMAL | EXCLUSIVE")
	radixSet(pragmaNames, "max_page_count", "N")
	radixSet(pragmaNames, "mmap_size", "N")
	radixSet(pragmaNames, "page_count", "") // no =
	radixSet(pragmaNames, "page_size", "bytes")
	//radixSet(pragmaNames,"parser_trace=", "boolean")
	radixSet(pragmaNames, "query_only", "boolean")
	radixSet(pragmaNames, "quick_check", "(N)") // no =
	radixSet(pragmaNames, "read_uncommitted", "boolean")
	radixSet(pragmaNames, "recursive_triggers", "boolean")
	radixSet(pragmaNames, "reverse_unordered_selects", "boolean")
	radixSet(pragmaNames, "schema_version", "integer")
	radixSet(pragmaNames, "secure_delete", "boolean")
	//radixSet(pragmaNames,"short_column_names", "boolean")
	radixSet(pragmaNames, "shrink_memory", "") // no =
	radixSet(pragmaNames, "soft_heap_limit", "N")
	//radixSet(pragmaNames,"stats", "")
	radixSet(pragmaNames, "synchronous", "0 | OFF | 1 | NORMAL | 2 | FULL")
	radixSet(pragmaNames, "table_info(", "table-name") // no =
	radixSet(pragmaNames, "temp_store", "0 | DEFAULT | 1 | FILE | 2 | MEMORY")
	//radixSet(pragmaNames,"temp_store_directory", "'directory-name'")
	radixSet(pragmaNames, "user_version", "integer")
	//radixSet(pragmaNames,"vdbe_addoptrace=", "boolean")
	//radixSet(pragmaNames,"vdbe_debug=", "boolean")
	//radixSet(pragmaNames,"vdbe_listing=", "boolean")
	//radixSet(pragmaNames,"vdbe_trace=", "boolean")
	radixSet(pragmaNames, "wal_autocheckpoint", "N")
	radixSet(pragmaNames, "wal_checkpoint", "(PASSIVE | FULL | RESTART)") // no =
	radixSet(pragmaNames, "writable_schema=", "boolean")                  // set-only

	radixSet(funcNames, "abs(", "X")
	radixSet(funcNames, "changes()", "")
	radixSet(funcNames, "char(", "X1,X2,...,XN")
	radixSet(funcNames, "coalesce(", "X,Y,...")
	radixSet(funcNames, "glob(", "X,Y")
	radixSet(funcNames, "ifnull(", "X,Y")
	radixSet(funcNames, "instr(", "X,Y")
	radixSet(funcNames, "hex(", "X")
	radixSet(funcNames, "last_insert_rowid()", "")
	radixSet(funcNames, "length(", "X")
	radixSet(funcNames, "like(", "X,Y[,Z]")
	radixSet(funcNames, "likelihood(", "X,Y")
	radixSet(funcNames, "load_extension(", "X[,Y]")
	radixSet(funcNames, "lower(", "X")
	radixSet(funcNames, "ltrim(", "X[,Y]")
	radixSet(funcNames, "max(", "X[,Y,...]")
	radixSet(funcNames, "min(", "X[,Y,...]")
	radixSet(funcNames, "nullif(", "X,Y")
	radixSet(funcNames, "printf(", "FORMAT,...")
	radixSet(funcNames, "quote(", "X")
	radixSet(funcNames, "random()", "")
	radixSet(funcNames, "randomblob(", "N")
	radixSet(funcNames, "replace", "X,Y,Z")
	radixSet(funcNames, "round(", "X[,Y]")
	radixSet(funcNames, "rtrim(", "X[,Y]")
	radixSet(funcNames, "soundex(", "X")
	radixSet(funcNames, "sqlite_compileoption_get(", "N")
	radixSet(funcNames, "sqlite_compileoption_used(", "X")
	radixSet(funcNames, "sqlite_source_id()", "")
	radixSet(funcNames, "sqlite_version()", "")
	radixSet(funcNames, "substr(", "X,Y[,Z]")
	radixSet(funcNames, "total_changes()", "")
	radixSet(funcNames, "trim(", "X[,Y]")
	radixSet(funcNames, "typeof(", "X")
	radixSet(funcNames, "unlikely(", "X")
	radixSet(funcNames, "unicode(", "X")
	radixSet(funcNames, "upper(", "X")
	radixSet(funcNames, "zeroblob(", "N")
	// aggregate functions
	radixSet(funcNames, "avg(", "X")
	radixSet(funcNames, "count(", "X|*")
	radixSet(funcNames, "group_concat(", "X[,Y]")
	//radixSet(funcNames,"max(", "X")
	//radixSet(funcNames,"min(", "X")
	radixSet(funcNames, "sum(", "X")
	radixSet(funcNames, "total(", "X")
	// date functions
	radixSet(funcNames, "date(", "timestring, modifier, modifier, ...")
	radixSet(funcNames, "time(", "timestring, modifier, modifier, ...")
	radixSet(funcNames, "datetime(", "timestring, modifier, modifier, ...")
	radixSet(funcNames, "julianday(", "timestring, modifier, modifier, ...")
	radixSet(funcNames, "strftime(", "format, timestring, modifier, modifier, ...")

	radixSet(moduleNames, "fts3(", "")
	radixSet(moduleNames, "fts4(", "")
	radixSet(moduleNames, "rtree(", "")
}

type radixValue struct {
	name string
	desc string
}

func radixSet(r *radix.Radix, name string, desc string) {
	r.Set(name, radixValue{name, desc})
}

func CompletePragma(prefix string) []string {
	return complete(pragmaNames, prefix)
}
func CompleteFunc(prefix string) []string {
	return complete(funcNames, prefix)
}

func complete(root *radix.Radix, prefix string) []string {
	r := root.SubTreeWithPrefix(prefix)
	if r == nil {
		return nil
	}
	names := make([]string, 0, 5)
	names = getChildrenNames(r, names)
	sort.Strings(names)
	return names
}
func getChildrenNames(r *radix.Radix, names []string) []string {
	for _, c := range r.Children() {
		names = getChildrenNames(c, names)
	}

	v := r.Value()
	if v, ok := v.(radixValue); ok {
		names = append(names, v.name)
	}
	return names
}
