package postgres

import (
	"github.com/Tooooommy/builder/v9"
)

func DialectOptions() *builder.SQLDialectOptions {
	do := builder.DefaultDialectOptions()
	do.PlaceHolderFragment = []byte("$")
	do.IncludePlaceholderNum = true
	return do
}

func init() {
	builder.RegisterDialect("postgres", DialectOptions())
}
