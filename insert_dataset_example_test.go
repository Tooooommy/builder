// nolint:lll // SQL statements are long
package builder_test

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/Tooooommy/builder/v9"
	_ "github.com/Tooooommy/builder/v9/dialect/postgres"
)

func ExampleInsert_builderRecord() {
	ds := builder.Insert("user").Rows(
		builder.Record{"first_name": "Greg", "last_name": "Farley"},
		builder.Record{"first_name": "Jimmy", "last_name": "Stewart"},
		builder.Record{"first_name": "Jeff", "last_name": "Jeffers"},
	)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("first_name", "last_name") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
}

func ExampleInsert_map() {
	ds := builder.Insert("user").Rows(
		map[string]any{"first_name": "Greg", "last_name": "Farley"},
		map[string]any{"first_name": "Jimmy", "last_name": "Stewart"},
		map[string]any{"first_name": "Jeff", "last_name": "Jeffers"},
	)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("first_name", "last_name") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
}

func ExampleInsert_struct() {
	type User struct {
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
	}
	ds := builder.Insert("user").Rows(
		User{FirstName: "Greg", LastName: "Farley"},
		User{FirstName: "Jimmy", LastName: "Stewart"},
		User{FirstName: "Jeff", LastName: "Jeffers"},
	)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("first_name", "last_name") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
}

func ExampleInsert_prepared() {
	ds := builder.Insert("user").Prepared(true).Rows(
		builder.Record{"first_name": "Greg", "last_name": "Farley"},
		builder.Record{"first_name": "Jimmy", "last_name": "Stewart"},
		builder.Record{"first_name": "Jeff", "last_name": "Jeffers"},
	)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("first_name", "last_name") VALUES (?, ?), (?, ?), (?, ?) [Greg Farley Jimmy Stewart Jeff Jeffers]
}

func ExampleInsert_fromQuery() {
	ds := builder.Insert("user").Prepared(true).
		FromQuery(builder.From("other_table"))
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" SELECT * FROM "other_table" []
}

func ExampleInsert_fromQueryWithCols() {
	ds := builder.Insert("user").Prepared(true).
		Cols("first_name", "last_name").
		FromQuery(builder.From("other_table").Select("fn", "ln"))
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("first_name", "last_name") SELECT "fn", "ln" FROM "other_table" []
}

func ExampleInsert_colsAndVals() {
	ds := builder.Insert("user").
		Cols("first_name", "last_name").
		Vals(
			builder.Vals{"Greg", "Farley"},
			builder.Vals{"Jimmy", "Stewart"},
			builder.Vals{"Jeff", "Jeffers"},
		)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("first_name", "last_name") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
}

func ExampleInsertDataset_Executor_withRecord() {
	db := getDB()
	insert := db.Insert("builder_user").Rows(
		builder.Record{"first_name": "Jed", "last_name": "Riley", "created": time.Now()},
	)
	if _, err := insert.Exec(); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Inserted 1 user")
	}

	users := []builder.Record{
		{"first_name": "Greg", "last_name": "Farley", "created": time.Now()},
		{"first_name": "Jimmy", "last_name": "Stewart", "created": time.Now()},
		{"first_name": "Jeff", "last_name": "Jeffers", "created": time.Now()},
	}
	if _, err := db.Insert("builder_user").Rows(users).Exec(); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("Inserted %d users", len(users))
	}

	// Output:
	// Inserted 1 user
	// Inserted 3 users
}

func ExampleInsertDataset_Executor_recordReturning() {
	db := getDB()

	type User struct {
		ID        sql.NullInt64 `db:"id"`
		FirstName string        `db:"first_name"`
		LastName  string        `db:"last_name"`
		Created   time.Time     `db:"created"`
	}

	insert := db.Insert("builder_user").Rows(
		builder.Record{"first_name": "Jed", "last_name": "Riley", "created": time.Now()},
	)

	if res, err := insert.Exec(); err != nil {
		fmt.Println(err.Error())
	} else {
		id, err := res.LastInsertId()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Inserted 1 user id:=%d\n", id)
	}

	records := []builder.Record{
		{"first_name": "Greg", "last_name": "Farley", "created": time.Now()},
		{"first_name": "Jimmy", "last_name": "Stewart", "created": time.Now()},
		{"first_name": "Jeff", "last_name": "Jeffers", "created": time.Now()},
	}
	for _, record := range records {
		ret, err := db.Insert("builder_user").Rows(record).Exec()
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		id, err := ret.LastInsertId()
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("Inserted user: [ID=%d]\n", id)
	}

	// Output:
	// Inserted 1 user id:=5
	// Inserted user: [ID=6]
	// Inserted user: [ID=7]
	// Inserted user: [ID=8]
}

func ExampleInsertDataset_Executor_QueryRows() {
	db := getDB()

	type User struct {
		ID        sql.NullInt64 `db:"id" builder:"skipinsert"`
		FirstName string        `db:"first_name"`
		LastName  string        `db:"last_name"`
		Created   time.Time     `db:"created"`
	}

	insert := db.Insert("builder_user").Rows(
		User{FirstName: "Jed", LastName: "Riley", Created: time.Now()},
	)
	if ret, err := insert.Exec(); err != nil {
		fmt.Println(err.Error())
	} else {
		id, err := ret.LastInsertId()
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("Inserted 1 user id:=%d\n", id)
	}

	users := []User{
		{FirstName: "Greg", LastName: "Farley", Created: time.Now()},
		{FirstName: "Jimmy", LastName: "Stewart", Created: time.Now()},
		{FirstName: "Jeff", LastName: "Jeffers", Created: time.Now()},
	}

	for _, user := range users {
		ret, err := db.Insert("builder_user").Rows(user).Exec()
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		id, err := ret.LastInsertId()
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		fmt.Printf("Inserted user: [ID=%d]\n", id)
	}

	// Output:
	// Inserted 1 user id:=5
	// Inserted user: [ID=6]
	// Inserted user: [ID=7]
	// Inserted user: [ID=8]
}

func ExampleInsertDataset_FromQuery() {
	insertSQL, _, _ := builder.Insert("test").
		FromQuery(builder.From("test2").Where(builder.C("age").Gt(10))).
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test" SELECT * FROM "test2" WHERE ("age" > 10)
}

func ExampleInsertDataset_ToSQL() {
	type item struct {
		ID      uint32 `db:"id" builder:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	insertSQL, args, _ := builder.Insert("items").Rows(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	).ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = builder.Insert("items").Rows(
		builder.Record{"name": "Test1", "address": "111 Test Addr"},
		builder.Record{"name": "Test2", "address": "112 Test Addr"},
	).ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = builder.Insert("items").Rows(
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		}).ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = builder.From("items").Insert().Rows(
		[]builder.Record{
			{"name": "Test1", "address": "111 Test Addr"},
			{"name": "Test2", "address": "112 Test Addr"},
		}).ToSQL()
	fmt.Println(insertSQL, args)
	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
}

func ExampleInsertDataset_Prepared() {
	type item struct {
		ID      uint32 `db:"id" builder:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	insertSQL, args, _ := builder.Insert("items").Prepared(true).Rows(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	).ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = builder.Insert("items").Prepared(true).Rows(
		builder.Record{"name": "Test1", "address": "111 Test Addr"},
		builder.Record{"name": "Test2", "address": "112 Test Addr"},
	).ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = builder.Insert("items").Prepared(true).Rows(
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		}).ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = builder.Insert("items").Prepared(true).Rows(
		[]builder.Record{
			{"name": "Test1", "address": "111 Test Addr"},
			{"name": "Test2", "address": "112 Test Addr"},
		}).ToSQL()
	fmt.Println(insertSQL, args)
	// Output:
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
}

func ExampleInsertDataset_ClearRows() {
	type item struct {
		ID      uint32 `builder:"skipinsert"`
		Address string
		Name    string
	}
	ds := builder.Insert("items").Rows(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	)
	insertSQL, args, _ := ds.ClearRows().ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "items" DEFAULT VALUES []
}

func ExampleInsertDataset_Rows_withNoDbTag() {
	type item struct {
		ID      uint32 `builder:"skipinsert"`
		Address string
		Name    string
	}
	insertSQL, args, _ := builder.Insert("items").
		Rows(
			item{Name: "Test1", Address: "111 Test Addr"},
			item{Name: "Test2", Address: "112 Test Addr"},
		).
		ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = builder.Insert("items").
		Rows(
			item{Name: "Test1", Address: "111 Test Addr"},
			item{Name: "Test2", Address: "112 Test Addr"},
		).
		ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = builder.Insert("items").
		Rows([]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		}).
		ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
}

func ExampleInsertDataset_Rows_withbuilderSkipInsertTag() {
	type item struct {
		ID      uint32 `builder:"skipinsert"`
		Address string
		Name    string `builder:"skipinsert"`
	}
	insertSQL, args, _ := builder.Insert("items").
		Rows(
			item{Name: "Test1", Address: "111 Test Addr"},
			item{Name: "Test2", Address: "112 Test Addr"},
		).
		ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = builder.Insert("items").
		Rows([]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		}).
		ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "items" ("address") VALUES ('111 Test Addr'), ('112 Test Addr') []
	// INSERT INTO "items" ("address") VALUES ('111 Test Addr'), ('112 Test Addr') []
}

func ExampleInsertDataset_Rows_withbuilderDefaultIfEmptyTag() {
	type item struct {
		ID      uint32 `builder:"skipinsert"`
		Address string
		Name    string `builder:"defaultifempty"`
	}
	insertSQL, args, _ := builder.Insert("items").
		Rows(
			item{Name: "Test1", Address: "111 Test Addr"},
			item{Address: "112 Test Addr"},
		).
		ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = builder.Insert("items").
		Rows([]item{
			{Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		}).
		ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', DEFAULT) []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', DEFAULT), ('112 Test Addr', 'Test2') []
}

func ExampleInsertDataset_Rows_withEmbeddedStruct() {
	type Address struct {
		Street string `db:"address_street"`
		State  string `db:"address_state"`
	}
	type User struct {
		Address
		FirstName string
		LastName  string
	}
	ds := builder.Insert("user").Rows(
		User{Address: Address{Street: "111 Street", State: "NY"}, FirstName: "Greg", LastName: "Farley"},
		User{Address: Address{Street: "211 Street", State: "NY"}, FirstName: "Jimmy", LastName: "Stewart"},
		User{Address: Address{Street: "311 Street", State: "NY"}, FirstName: "Jeff", LastName: "Jeffers"},
	)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("address_state", "address_street", "firstname", "lastname") VALUES ('NY', '111 Street', 'Greg', 'Farley'), ('NY', '211 Street', 'Jimmy', 'Stewart'), ('NY', '311 Street', 'Jeff', 'Jeffers') []
}

func ExampleInsertDataset_Rows_withIgnoredEmbedded() {
	type Address struct {
		Street string
		State  string
	}
	type User struct {
		Address   `db:"-"`
		FirstName string
		LastName  string
	}
	ds := builder.Insert("user").Rows(
		User{Address: Address{Street: "111 Street", State: "NY"}, FirstName: "Greg", LastName: "Farley"},
		User{Address: Address{Street: "211 Street", State: "NY"}, FirstName: "Jimmy", LastName: "Stewart"},
		User{Address: Address{Street: "311 Street", State: "NY"}, FirstName: "Jeff", LastName: "Jeffers"},
	)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("firstname", "lastname") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
}

func ExampleInsertDataset_Rows_withNilEmbeddedPointer() {
	type Address struct {
		Street string
		State  string
	}
	type User struct {
		*Address
		FirstName string
		LastName  string
	}
	ds := builder.Insert("user").Rows(
		User{FirstName: "Greg", LastName: "Farley"},
		User{FirstName: "Jimmy", LastName: "Stewart"},
		User{FirstName: "Jeff", LastName: "Jeffers"},
	)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("firstname", "lastname") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
}

func ExampleInsertDataset_ClearOnConflict() {
	type item struct {
		ID      uint32 `db:"id" builder:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds := builder.Insert("items").OnConflict(builder.DoNothing())
	insertSQL, args, _ := ds.ClearOnConflict().Rows(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	).ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
}

func ExampleInsertDataset_OnConflict_doNothing() {
	type item struct {
		ID      uint32 `db:"id" builder:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	insertSQL, args, _ := builder.Insert("items").Rows(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	).OnConflict(builder.DoNothing()).ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') ON CONFLICT DO NOTHING []
}

func ExampleInsertDataset_OnConflict_doUpdate() {
	insertSQL, args, _ := builder.Insert("items").
		Rows(
			builder.Record{"name": "Test1", "address": "111 Test Addr"},
			builder.Record{"name": "Test2", "address": "112 Test Addr"},
		).
		OnConflict(builder.DoUpdate("key", builder.Record{"updated": builder.L("NOW()")})).
		ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') ON CONFLICT (key) DO UPDATE SET "updated"=NOW() []
}

func ExampleInsertDataset_OnConflict_doUpdateWithWhere() {
	type item struct {
		ID      uint32 `db:"id" builder:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	insertSQL, args, _ := builder.Insert("items").
		Rows([]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		}).
		OnConflict(builder.DoUpdate(
			"key",
			builder.Record{"updated": builder.L("NOW()")}).Where(builder.C("allow_update").IsTrue()),
		).
		ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') ON CONFLICT (key) DO UPDATE SET "updated"=NOW() WHERE ("allow_update" IS TRUE) []
}

func ExampleInsertDataset_Returning() {
	insertSQL, _, _ := builder.Insert("test").
		Returning("id").
		Rows(builder.Record{"a": "a", "b": "b"}).
		ToSQL()
	fmt.Println(insertSQL)
	insertSQL, _, _ = builder.Insert("test").
		Returning(builder.T("test").All()).
		Rows(builder.Record{"a": "a", "b": "b"}).
		ToSQL()
	fmt.Println(insertSQL)
	insertSQL, _, _ = builder.Insert("test").
		Returning("a", "b").
		Rows(builder.Record{"a": "a", "b": "b"}).
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "id"
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "test".*
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "a", "b"
}

func ExampleInsertDataset_With() {
	insertSQL, _, _ := builder.Insert("foo").
		With("other", builder.From("bar").Where(builder.C("id").Gt(10))).
		FromQuery(builder.From("other")).
		ToSQL()
	fmt.Println(insertSQL)

	// Output:
	// WITH other AS (SELECT * FROM "bar" WHERE ("id" > 10)) INSERT INTO "foo" SELECT * FROM "other"
}

func ExampleInsertDataset_WithRecursive() {
	insertSQL, _, _ := builder.Insert("num_count").
		WithRecursive("nums(x)",
			builder.From().Select(builder.L("1")).
				UnionAll(builder.From("nums").
					Select(builder.L("x+1")).Where(builder.C("x").Lt(5))),
		).
		FromQuery(builder.From("nums")).
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// WITH RECURSIVE nums(x) AS (SELECT 1 UNION ALL (SELECT x+1 FROM "nums" WHERE ("x" < 5))) INSERT INTO "num_count" SELECT * FROM "nums"
}

func ExampleInsertDataset_Into() {
	ds := builder.Insert("test")
	insertSQL, _, _ := ds.Into("test2").Rows(builder.Record{"first_name": "bob", "last_name": "yukon"}).ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test2" ("first_name", "last_name") VALUES ('bob', 'yukon')
}

func ExampleInsertDataset_Into_aliased() {
	ds := builder.Insert("test")
	insertSQL, _, _ := ds.
		Into(builder.T("test").As("t")).
		Rows(builder.Record{"first_name": "bob", "last_name": "yukon"}).
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test" AS "t" ("first_name", "last_name") VALUES ('bob', 'yukon')
}

func ExampleInsertDataset_Cols() {
	insertSQL, _, _ := builder.Insert("test").
		Cols("a", "b", "c").
		Vals(
			[]any{"a1", "b1", "c1"},
			[]any{"a2", "b1", "c1"},
			[]any{"a3", "b1", "c1"},
		).
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test" ("a", "b", "c") VALUES ('a1', 'b1', 'c1'), ('a2', 'b1', 'c1'), ('a3', 'b1', 'c1')
}

func ExampleInsertDataset_Cols_withFromQuery() {
	insertSQL, _, _ := builder.Insert("test").
		Cols("a", "b", "c").
		FromQuery(builder.From("foo").Select("d", "e", "f")).
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test" ("a", "b", "c") SELECT "d", "e", "f" FROM "foo"
}

func ExampleInsertDataset_ColsAppend() {
	insertSQL, _, _ := builder.Insert("test").
		Cols("a", "b").
		ColsAppend("c").
		Vals(
			[]any{"a1", "b1", "c1"},
			[]any{"a2", "b1", "c1"},
			[]any{"a3", "b1", "c1"},
		).
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test" ("a", "b", "c") VALUES ('a1', 'b1', 'c1'), ('a2', 'b1', 'c1'), ('a3', 'b1', 'c1')
}

func ExampleInsertDataset_ClearCols() {
	ds := builder.Insert("test").Cols("a", "b", "c")
	insertSQL, _, _ := ds.ClearCols().Cols("other_a", "other_b", "other_c").
		FromQuery(builder.From("foo").Select("d", "e", "f")).
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test" ("other_a", "other_b", "other_c") SELECT "d", "e", "f" FROM "foo"
}

func ExampleInsertDataset_Vals() {
	insertSQL, _, _ := builder.Insert("test").
		Cols("a", "b", "c").
		Vals(
			[]any{"a1", "b1", "c1"},
			[]any{"a2", "b2", "c2"},
			[]any{"a3", "b3", "c3"},
		).
		ToSQL()
	fmt.Println(insertSQL)

	insertSQL, _, _ = builder.Insert("test").
		Cols("a", "b", "c").
		Vals([]any{"a1", "b1", "c1"}).
		Vals([]any{"a2", "b2", "c2"}).
		Vals([]any{"a3", "b3", "c3"}).
		ToSQL()
	fmt.Println(insertSQL)

	// Output:
	// INSERT INTO "test" ("a", "b", "c") VALUES ('a1', 'b1', 'c1'), ('a2', 'b2', 'c2'), ('a3', 'b3', 'c3')
	// INSERT INTO "test" ("a", "b", "c") VALUES ('a1', 'b1', 'c1'), ('a2', 'b2', 'c2'), ('a3', 'b3', 'c3')
}

func ExampleInsertDataset_ClearVals() {
	insertSQL, _, _ := builder.Insert("test").
		Cols("a", "b", "c").
		Vals(
			[]any{"a1", "b1", "c1"},
			[]any{"a2", "b1", "c1"},
			[]any{"a3", "b1", "c1"},
		).
		ClearVals().
		ToSQL()
	fmt.Println(insertSQL)

	insertSQL, _, _ = builder.Insert("test").
		Cols("a", "b", "c").
		Vals([]any{"a1", "b1", "c1"}).
		Vals([]any{"a2", "b2", "c2"}).
		Vals([]any{"a3", "b3", "c3"}).
		ClearVals().
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test" DEFAULT VALUES
	// INSERT INTO "test" DEFAULT VALUES
}
