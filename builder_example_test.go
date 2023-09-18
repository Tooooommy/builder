package builder_test

import (
	"fmt"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Tooooommy/builder/v9"
	_ "github.com/Tooooommy/builder/v9/dialect/mysql"
	_ "github.com/Tooooommy/builder/v9/dialect/postgres"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

// Creating a mysql dataset. Be sure to import the mysql adapter.
func ExampleDialect_datasetMysql() {
	// import _ "github.com/Tooooommy/builder/v9/dialect/mysql"

	d := builder.Dialect("mysql")
	ds := d.From("test").Where(builder.Ex{
		"foo": "bar",
		"baz": []int64{1, 2, 3},
	}).Limit(10)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM `test` WHERE ((`baz` IN (1, 2, 3)) AND (`foo` = 'bar')) LIMIT 10 []
	// SELECT * FROM `test` WHERE ((`baz` IN (?, ?, ?)) AND (`foo` = ?)) LIMIT ? [1 2 3 bar 10]
}

// Creating a mysql database. Be sure to import the mysql adapter.
func ExampleDialect_dbMysql() {
	// import _ "github.com/Tooooommy/builder/v9/dialect/mysql"

	type item struct {
		ID      int64  `db:"id"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	// set up a mock db this would normally be
	// db, err := sql.Open("mysql", dbURI)
	// 	if err != nil {
	// 		panic(err.Error())
	// 	}
	mDB, mock, _ := sqlmock.New()

	d := builder.Dialect("mysql")

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := d.DB(conn)

	// use the db.From to get a dataset to execute queries
	ds := db.From("items").Where(builder.C("id").Eq(1))

	// set up mock for example purposes
	mock.ExpectQuery("SELECT \\* FROM `items` WHERE \\(`id` = 1\\) LIMIT 1").
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "address", "name"}).
				FromCSVString("1, 111 Test Addr,Test1"),
		)
	var it item
	err := ds.QueryRow(&it)
	fmt.Println(it, err)

	// set up mock for example purposes
	mock.ExpectQuery("SELECT \\* FROM `items` WHERE \\(`id` = \\?\\) LIMIT \\?").
		WithArgs(1, 1).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "address", "name"}).
				FromCSVString("1, 111 Test Addr,Test1"),
		)

	err = ds.Prepared(true).QueryRow(&it)
	fmt.Println(it, err)

	// Output:
	// {1 111 Test Addr Test1} <nil>
	// {1 111 Test Addr Test1} <nil>
}

// Creating a mysql dataset. Be sure to import the postgres adapter
func ExampleDialect_datasetPostgres() {
	// import _ "github.com/Tooooommy/builder/v9/dialect/postgres"

	d := builder.Dialect("postgres")
	ds := d.From("test").Where(builder.Ex{
		"foo": "bar",
		"baz": []int64{1, 2, 3},
	}).Limit(10)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE (("baz" IN (1, 2, 3)) AND ("foo" = 'bar')) LIMIT 10 []
	// SELECT * FROM "test" WHERE (("baz" IN ($1, $2, $3)) AND ("foo" = $4)) LIMIT $5 [1 2 3 bar 10]
}

// Creating a postgres dataset. Be sure to import the postgres adapter
func ExampleDialect_dbPostgres() {
	// import _ "github.com/Tooooommy/builder/v9/dialect/postgres"

	type item struct {
		ID      int64  `db:"id"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	// set up a mock db this would normally be
	// db, err := sql.Open("postgres", dbURI)
	// 	if err != nil {
	// 		panic(err.Error())
	// 	}
	mDB, mock, _ := sqlmock.New()

	d := builder.Dialect("postgres")

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := d.DB(conn)

	// use the db.From to get a dataset to execute queries
	ds := db.From("items").Where(builder.C("id").Eq(1))

	// set up mock for example purposes
	mock.ExpectQuery(`SELECT \* FROM "items" WHERE \("id" = 1\) LIMIT 1`).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "address", "name"}).
				FromCSVString("1, 111 Test Addr,Test1"),
		)
	var it item
	err := ds.QueryRow(&it)
	fmt.Println(it, err)

	// set up mock for example purposes
	mock.ExpectQuery(`SELECT \* FROM "items" WHERE \("id" = \$1\) LIMIT \$2`).
		WithArgs(1, 1).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "address", "name"}).
				FromCSVString("1, 111 Test Addr,Test1"),
		)

	err = ds.Prepared(true).QueryRow(&it)
	fmt.Println(it, err)

	// Output:
	// {1 111 Test Addr Test1} <nil>
	// {1 111 Test Addr Test1} <nil>
}

// Creating a mysql dataset. Be sure to import the sqlite3 adapter
func ExampleDialect_datasetSqlite3() {
	// import _ "github.com/Tooooommy/builder/v9/dialect/sqlite3"

	d := builder.Dialect("sqlite3")
	ds := d.From("test").Where(builder.Ex{
		"foo": "bar",
		"baz": []int64{1, 2, 3},
	}).Limit(10)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE (("baz" IN (1, 2, 3)) AND ("foo" = 'bar')) LIMIT 10 []
	// SELECT * FROM "test" WHERE (("baz" IN (?, ?, ?)) AND ("foo" = ?)) LIMIT ? [1 2 3 bar 10]
}

// Creating a sqlite3 database. Be sure to import the sqlite3 adapter
func ExampleDialect_dbSqlite3() {
	// import _ "github.com/Tooooommy/builder/v9/dialect/sqlite3"
	type item struct {
		ID      int64  `db:"id"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	// set up a mock db this would normally be
	// db, err := sql.Open("sqlite3", dbURI)
	// 	if err != nil {
	// 		panic(err.Error())
	// 	}
	mDB, mock, _ := sqlmock.New()

	d := builder.Dialect("sqlite3")

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := d.DB(conn)

	// use the db.From to get a dataset to execute queries
	ds := db.From("items").Where(builder.C("id").Eq(1))

	// set up mock for example purposes
	mock.ExpectQuery(`SELECT \* FROM "items" WHERE \("id" = 1\) LIMIT 1`).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "address", "name"}).
				FromCSVString("1, 111 Test Addr,Test1"),
		)
	var it item
	err := ds.QueryRow(&it)
	fmt.Println(it, err)

	// set up mock for example purposes
	mock.ExpectQuery(`SELECT \* FROM "items" WHERE \("id" = \?\) LIMIT \?`).
		WithArgs(1, 1).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "address", "name"}).
				FromCSVString("1, 111 Test Addr,Test1"),
		)

	err = ds.Prepared(true).QueryRow(&it)
	fmt.Println(it, err)

	// Output:
	// {1 111 Test Addr Test1} <nil>
	// {1 111 Test Addr Test1} <nil>
}

func ExampleSetTimeLocation() {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		panic(err)
	}

	created, err := time.Parse(time.RFC3339, "2019-10-01T15:01:00Z")
	if err != nil {
		panic(err)
	}

	// use original time with tz info
	builder.SetTimeLocation(loc)
	ds := builder.Insert("test").Rows(builder.Record{
		"address": "111 Address",
		"name":    "Bob Yukon",
		"created": created,
	})
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)

	// convert time to UTC
	builder.SetTimeLocation(time.UTC)
	sql, _, _ = ds.ToSQL()
	fmt.Println(sql)

	// Output:
	// INSERT INTO "test" ("address", "created", "name") VALUES ('111 Address', '2019-10-01T23:01:00+08:00', 'Bob Yukon')
	// INSERT INTO "test" ("address", "created", "name") VALUES ('111 Address', '2019-10-01T15:01:00Z', 'Bob Yukon')
}
