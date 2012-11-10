Yet another SQLite binding based on:
 - original [Russ Cox's](http://code.google.com/p/gosqlite/) implementation,
 - the [Patrick Crosby's](https://github.com/patrickxb/fgosqlite/) fork.
This binding implements the "database/sql/driver" interface.

See [package documentation](http://go.pkgdoc.org/github.com/gwenn/gosqlite).

Open supports flags.  
Conn#Exec handles multiple statements (separated by semicolons) properly.  
Conn#Prepare can optionnaly #Bind as well.  
Conn#Prepare can reuse already prepared Stmt.  
Conn#Close ensures that all dangling statements are finalized.  
Stmt#Exec is renamed in Stmt#Bind and a new Stmt#Exec method is introduced to #Bind and #Step.  
Stmt#Bind uses native sqlite3_bind_x methods and failed if unsupported type.  
Stmt#NamedBind can be used to bind by name.  
Stmt#Next returns a (bool, os.Error) couple like Reader#Read.  
Stmt#Scan uses native sqlite3_column_x methods.  
Stmt#NamedScan is added. It's compliant with [go-dbi](https://github.com/thomaslee/go-dbi/).  
Stmt#ScanByIndex/ScanByName are added to test NULL value.

Currently, the weak point of the binding is the *Scan* methods:
The original implementation is using this strategy:
 - convert the stored value to a []byte by calling sqlite3_column_blob,
 - convert the bytes to the desired Go type with correct feedback in case of illegal conversion,
 - but apparently no support for NULL value.
Using the native sqlite3_column_x implies:
 - optimal conversion from the storage type to Go type (when they match),
 - loosy conversion when types mismatch (select cast('M' as int); --> 0),
 - NULL value can be returned only for **type, otherwise a default value (0, false, "") is returned.

SQLite logs (SQLITE_CONFIG_LOG) can be activated by:
- ConfigLog function
- or `export SQLITE_LOG=1`

Misc:  
Conn#Exists  
Conn#OneValue  

Conn#OpenVfs  
Conn#EnableFkey/IsFKeyEnabled  
Conn#Changes/TotalChanges  
Conn#LastInsertRowid  
Conn#Interrupt  
Conn#Begin/BeginTransaction(type)/Commit/Rollback  
Conn#GetAutocommit  
Conn#EnableLoadExtension/LoadExtension  
Conn#IntegrityCheck  

Stmt#Insert/ExecDml/Select/SelectOneRow  
Stmt#BindParameterCount/BindParameterIndex(name)/BindParameterName(index)  
Stmt#ClearBindings  
Stmt#ColumnCount/ColumnNames/ColumnIndex(name)/ColumnName(index)/ColumnType(index)  
Stmt#ReadOnly  
Stmt#Busy  

Blob:  
ZeroBlobLength  
Conn#NewBlobReader  
Conn#NewBlobReadWriter  

Meta:  
Conn#Databases  
Conn#Tables  
Conn#Columns  
Conn#ForeignKeys  
Conn#Indexes/IndexColumns  

Time:  
JulianDay  
JulianDayToUTC  
JulianDayToLocalTime  

Trace:  
Conn#BusyHandler  
Conn#Profile  
Conn#ProgressHandler  
Conn#SetAuthorizer  
Conn#Trace  
Stmt#Status  

Hook:  
Conn#CommitHook  
Conn#RollbackHook  
Conn#UpdateHook  

Function:  
Conn#CreateScalarFunction  
Conn#CreateAggregateFunction  

$ go test -test.bench '.*'
<pre>
BenchmarkValuesScan 500000  4658 ns/op
BenchmarkScan       500000   324 ns/op
BenchmarkNamedScan  200000  9221 ns/op

BenchmarkInsert       500000  6088 ns/op
BenchmarkNamedInsert  500000  6726 ns/op

BenchmarkDisabledCache   100000  19235 ns/op
BenchmarkEnabledCache   1000000   1133 ns/op

BenchmarkLike   1000000  2508 ns/op
BenchmarkHalf    500000  4811 ns/op
BenchmarkRegexp  500000  6170 ns/op
</pre>