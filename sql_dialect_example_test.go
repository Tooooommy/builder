package builder_test

import (
	"fmt"

	"github.com/Tooooommy/builder/v9"
)

func ExampleRegisterDialect() {
	opts := builder.DefaultDialectOptions()
	opts.QuoteRune = '`'
	builder.RegisterDialect("custom-dialect", opts)

	dialect := builder.Dialect("custom-dialect")

	ds := dialect.From("test")

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM `test` []
}
