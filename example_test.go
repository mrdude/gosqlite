package sqlite_test

import (
	"fmt"
	"github.com/gwenn/gosqlite"
)

// 0
func ExampleOpen() {
	db, err := sqlite.Open(":memory:")
	if err != nil {
		// ...
	}
	defer db.Close()
	fmt.Printf("%d\n", db.TotalChanges())
}
