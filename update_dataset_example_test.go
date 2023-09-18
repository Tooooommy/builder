// nolint:lll // sql statements are long
package builder_test

import (
	"fmt"

	"github.com/Tooooommy/builder/v9"
	_ "github.com/Tooooommy/builder/v9/dialect/mysql"
)

func ExampleUpdate_withStruct() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := builder.Update("items").Set(
		item{Name: "Test", Address: "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
}

func ExampleUpdate_withbuilderRecord() {
	sql, args, _ := builder.Update("items").Set(
		builder.Record{"name": "Test", "address": "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
}

func ExampleUpdate_withMap() {
	sql, args, _ := builder.Update("items").Set(
		map[string]any{"name": "Test", "address": "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
}

func ExampleUpdate_withSkipUpdateTag() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name" builder:"skipupdate"`
	}
	sql, args, _ := builder.Update("items").Set(
		item{Name: "Test", Address: "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// UPDATE "items" SET "address"='111 Test Addr' []
}

func ExampleUpdateDataset_Exec() {
	db := getDB()
	_, err := db.Update("builder_user").
		Set(builder.Record{"last_name": "ucon"}).
		Where(builder.Ex{"last_name": "Yukon"}).Exec()

	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("UpdateDatabaset Exec")
	}

	// Output:
	// UpdateDatabaset Exec
}

func ExampleUpdateDataset_ToSQL() {
	sql, _, _ := builder.Update("test").
		Set(builder.Record{"foo": "bar"}).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = builder.Update("test").
		Set(builder.Record{"foo": "bar"}).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = builder.Update("test").
		Set(builder.Record{"foo": "bar"}).
		ToSQL()
	fmt.Println(sql)
	// Output:
	// UPDATE "test" SET "foo"='bar'
	// UPDATE "test" SET "foo"='bar'
	// UPDATE "test" SET "foo"='bar'
}

func ExampleUpdateDataset_With() {
	sql, _, _ := builder.Update("test").
		With("some_vals(val)", builder.From().Select(builder.L("123"))).
		Where(builder.C("val").Eq(builder.From("some_vals").Select("val"))).
		Set(builder.Record{"name": "Test"}).ToSQL()
	fmt.Println(sql)

	// Output:
	// WITH some_vals(val) AS (SELECT 123) UPDATE "test" SET "name"='Test' WHERE ("val" IN (SELECT "val" FROM "some_vals"))
}

func ExampleUpdateDataset_WithRecursive() {
	sql, _, _ := builder.Update("nums").
		WithRecursive("nums(x)", builder.From().Select(builder.L("1").As("num")).
			UnionAll(builder.From("nums").
				Select(builder.L("x+1").As("num")).Where(builder.C("x").Lt(5)))).
		Set(builder.Record{"foo": builder.T("nums").Col("num")}).
		ToSQL()
	fmt.Println(sql)
	// Output:
	// WITH RECURSIVE nums(x) AS (SELECT 1 AS "num" UNION ALL (SELECT x+1 AS "num" FROM "nums" WHERE ("x" < 5))) UPDATE "nums" SET "foo"="nums"."num"
}

func ExampleUpdateDataset_Limit() {
	ds := builder.Dialect("mysql").
		Update("test").
		Set(builder.Record{"foo": "bar"}).
		Limit(10)
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// UPDATE `test` SET `foo`='bar' LIMIT 10
}

func ExampleUpdateDataset_LimitAll() {
	ds := builder.Dialect("mysql").
		Update("test").
		Set(builder.Record{"foo": "bar"}).
		LimitAll()
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// UPDATE `test` SET `foo`='bar' LIMIT ALL
}

func ExampleUpdateDataset_ClearLimit() {
	ds := builder.Dialect("mysql").
		Update("test").
		Set(builder.Record{"foo": "bar"}).
		Limit(10)
	sql, _, _ := ds.ClearLimit().ToSQL()
	fmt.Println(sql)
	// Output:
	// UPDATE `test` SET `foo`='bar'
}

func ExampleUpdateDataset_Order() {
	ds := builder.Dialect("mysql").
		Update("test").
		Set(builder.Record{"foo": "bar"}).
		Order(builder.C("a").Asc())
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// UPDATE `test` SET `foo`='bar' ORDER BY `a` ASC
}

func ExampleUpdateDataset_OrderAppend() {
	ds := builder.Dialect("mysql").
		Update("test").
		Set(builder.Record{"foo": "bar"}).
		Order(builder.C("a").Asc())
	sql, _, _ := ds.OrderAppend(builder.C("b").Desc().NullsLast()).ToSQL()
	fmt.Println(sql)
	// Output:
	// UPDATE `test` SET `foo`='bar' ORDER BY `a` ASC, `b` DESC NULLS LAST
}

func ExampleUpdateDataset_OrderPrepend() {
	ds := builder.Dialect("mysql").
		Update("test").
		Set(builder.Record{"foo": "bar"}).
		Order(builder.C("a").Asc())

	sql, _, _ := ds.OrderPrepend(builder.C("b").Desc().NullsLast()).ToSQL()
	fmt.Println(sql)
	// Output:
	// UPDATE `test` SET `foo`='bar' ORDER BY `b` DESC NULLS LAST, `a` ASC
}

func ExampleUpdateDataset_ClearOrder() {
	ds := builder.Dialect("mysql").
		Update("test").
		Set(builder.Record{"foo": "bar"}).
		Order(builder.C("a").Asc())
	sql, _, _ := ds.ClearOrder().ToSQL()
	fmt.Println(sql)
	// Output:
	// UPDATE `test` SET `foo`='bar'
}

func ExampleUpdateDataset_From() {
	ds := builder.Update("table_one").
		Set(builder.Record{"foo": builder.I("table_two.bar")}).
		From("table_two").
		Where(builder.Ex{"table_one.id": builder.I("table_two.id")})

	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// UPDATE "table_one" SET "foo"="table_two"."bar" FROM "table_two" WHERE ("table_one"."id" = "table_two"."id")
}

func ExampleUpdateDataset_From_postgres() {
	dialect := builder.Dialect("postgres")

	ds := dialect.Update("table_one").
		Set(builder.Record{"foo": builder.I("table_two.bar")}).
		From("table_two").
		Where(builder.Ex{"table_one.id": builder.I("table_two.id")})

	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// UPDATE "table_one" SET "foo"="table_two"."bar" FROM "table_two" WHERE ("table_one"."id" = "table_two"."id")
}

func ExampleUpdateDataset_From_mysql() {
	dialect := builder.Dialect("mysql")

	ds := dialect.Update("table_one").
		Set(builder.Record{"foo": builder.I("table_two.bar")}).
		From("table_two").
		Where(builder.Ex{"table_one.id": builder.I("table_two.id")})

	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// UPDATE `table_one`,`table_two` SET `foo`=`table_two`.`bar` WHERE (`table_one`.`id` = `table_two`.`id`)
}

func ExampleUpdateDataset_Where() {
	// By default everything is anded together
	sql, _, _ := builder.Update("test").
		Set(builder.Record{"foo": "bar"}).
		Where(builder.Ex{
			"a": builder.Op{"gt": 10},
			"b": builder.Op{"lt": 10},
			"c": nil,
			"d": []string{"a", "b", "c"},
		}).ToSQL()
	fmt.Println(sql)
	// You can use ExOr to get ORed expressions together
	sql, _, _ = builder.Update("test").
		Set(builder.Record{"foo": "bar"}).
		Where(builder.ExOr{
			"a": builder.Op{"gt": 10},
			"b": builder.Op{"lt": 10},
			"c": nil,
			"d": []string{"a", "b", "c"},
		}).ToSQL()
	fmt.Println(sql)
	// You can use Or with Ex to Or multiple Ex maps together
	sql, _, _ = builder.Update("test").
		Set(builder.Record{"foo": "bar"}).
		Where(
			builder.Or(
				builder.Ex{
					"a": builder.Op{"gt": 10},
					"b": builder.Op{"lt": 10},
				},
				builder.Ex{
					"c": nil,
					"d": []string{"a", "b", "c"},
				},
			),
		).ToSQL()
	fmt.Println(sql)
	// By default everything is anded together
	sql, _, _ = builder.Update("test").
		Set(builder.Record{"foo": "bar"}).
		Where(
			builder.C("a").Gt(10),
			builder.C("b").Lt(10),
			builder.C("c").IsNull(),
			builder.C("d").In("a", "b", "c"),
		).ToSQL()
	fmt.Println(sql)
	// You can use a combination of Ors and Ands
	sql, _, _ = builder.Update("test").
		Set(builder.Record{"foo": "bar"}).
		Where(
			builder.Or(
				builder.C("a").Gt(10),
				builder.And(
					builder.C("b").Lt(10),
					builder.C("c").IsNull(),
				),
			),
		).ToSQL()
	fmt.Println(sql)
	// Output:
	// UPDATE "test" SET "foo"='bar' WHERE (("a" > 10) AND ("b" < 10) AND ("c" IS NULL) AND ("d" IN ('a', 'b', 'c')))
	// UPDATE "test" SET "foo"='bar' WHERE (("a" > 10) OR ("b" < 10) OR ("c" IS NULL) OR ("d" IN ('a', 'b', 'c')))
	// UPDATE "test" SET "foo"='bar' WHERE ((("a" > 10) AND ("b" < 10)) OR (("c" IS NULL) AND ("d" IN ('a', 'b', 'c'))))
	// UPDATE "test" SET "foo"='bar' WHERE (("a" > 10) AND ("b" < 10) AND ("c" IS NULL) AND ("d" IN ('a', 'b', 'c')))
	// UPDATE "test" SET "foo"='bar' WHERE (("a" > 10) OR (("b" < 10) AND ("c" IS NULL)))
}

func ExampleUpdateDataset_Where_prepared() {
	// By default everything is anded together
	sql, args, _ := builder.Update("test").
		Prepared(true).
		Set(builder.Record{"foo": "bar"}).
		Where(builder.Ex{
			"a": builder.Op{"gt": 10},
			"b": builder.Op{"lt": 10},
			"c": nil,
			"d": []string{"a", "b", "c"},
		}).ToSQL()
	fmt.Println(sql, args)
	// You can use ExOr to get ORed expressions together
	sql, args, _ = builder.Update("test").Prepared(true).
		Set(builder.Record{"foo": "bar"}).
		Where(builder.ExOr{
			"a": builder.Op{"gt": 10},
			"b": builder.Op{"lt": 10},
			"c": nil,
			"d": []string{"a", "b", "c"},
		}).ToSQL()
	fmt.Println(sql, args)
	// You can use Or with Ex to Or multiple Ex maps together
	sql, args, _ = builder.Update("test").Prepared(true).
		Set(builder.Record{"foo": "bar"}).
		Where(
			builder.Or(
				builder.Ex{
					"a": builder.Op{"gt": 10},
					"b": builder.Op{"lt": 10},
				},
				builder.Ex{
					"c": nil,
					"d": []string{"a", "b", "c"},
				},
			),
		).ToSQL()
	fmt.Println(sql, args)
	// By default everything is anded together
	sql, args, _ = builder.Update("test").Prepared(true).
		Set(builder.Record{"foo": "bar"}).
		Where(
			builder.C("a").Gt(10),
			builder.C("b").Lt(10),
			builder.C("c").IsNull(),
			builder.C("d").In("a", "b", "c"),
		).ToSQL()
	fmt.Println(sql, args)
	// You can use a combination of Ors and Ands
	sql, args, _ = builder.Update("test").Prepared(true).
		Set(builder.Record{"foo": "bar"}).
		Where(
			builder.Or(
				builder.C("a").Gt(10),
				builder.And(
					builder.C("b").Lt(10),
					builder.C("c").IsNull(),
				),
			),
		).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// UPDATE "test" SET "foo"=? WHERE (("a" > ?) AND ("b" < ?) AND ("c" IS NULL) AND ("d" IN (?, ?, ?))) [bar 10 10 a b c]
	// UPDATE "test" SET "foo"=? WHERE (("a" > ?) OR ("b" < ?) OR ("c" IS NULL) OR ("d" IN (?, ?, ?))) [bar 10 10 a b c]
	// UPDATE "test" SET "foo"=? WHERE ((("a" > ?) AND ("b" < ?)) OR (("c" IS NULL) AND ("d" IN (?, ?, ?)))) [bar 10 10 a b c]
	// UPDATE "test" SET "foo"=? WHERE (("a" > ?) AND ("b" < ?) AND ("c" IS NULL) AND ("d" IN (?, ?, ?))) [bar 10 10 a b c]
	// UPDATE "test" SET "foo"=? WHERE (("a" > ?) OR (("b" < ?) AND ("c" IS NULL))) [bar 10 10]
}

func ExampleUpdateDataset_ClearWhere() {
	ds := builder.
		Update("test").
		Set(builder.Record{"foo": "bar"}).
		Where(
			builder.Or(
				builder.C("a").Gt(10),
				builder.And(
					builder.C("b").Lt(10),
					builder.C("c").IsNull(),
				),
			),
		)
	sql, _, _ := ds.ClearWhere().ToSQL()
	fmt.Println(sql)
	// Output:
	// UPDATE "test" SET "foo"='bar'
}

func ExampleUpdateDataset_Table() {
	ds := builder.Update("test")
	sql, _, _ := ds.Table("test2").Set(builder.Record{"foo": "bar"}).ToSQL()
	fmt.Println(sql)
	// Output:
	// UPDATE "test2" SET "foo"='bar'
}

func ExampleUpdateDataset_Table_aliased() {
	ds := builder.Update("test")
	sql, _, _ := ds.Table(builder.T("test").As("t")).Set(builder.Record{"foo": "bar"}).ToSQL()
	fmt.Println(sql)
	// Output:
	// UPDATE "test" AS "t" SET "foo"='bar'
}

func ExampleUpdateDataset_Set() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := builder.Update("items").Set(
		item{Name: "Test", Address: "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.Update("items").Set(
		builder.Record{"name": "Test", "address": "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.Update("items").Set(
		map[string]any{"name": "Test", "address": "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
}

func ExampleUpdateDataset_Set_struct() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := builder.Update("items").Set(
		item{Name: "Test", Address: "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
}

func ExampleUpdateDataset_Set_builderRecord() {
	sql, args, _ := builder.Update("items").Set(
		builder.Record{"name": "Test", "address": "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
}

func ExampleUpdateDataset_Set_map() {
	sql, args, _ := builder.Update("items").Set(
		map[string]any{"name": "Test", "address": "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
}

func ExampleUpdateDataset_Set_withSkipUpdateTag() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name" builder:"skipupdate"`
	}
	sql, args, _ := builder.Update("items").Set(
		item{Name: "Test", Address: "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// UPDATE "items" SET "address"='111 Test Addr' []
}

func ExampleUpdateDataset_Set_withDefaultIfEmptyTag() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name" builder:"defaultifempty"`
	}
	sql, args, _ := builder.Update("items").Set(
		item{Address: "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.Update("items").Set(
		item{Name: "Bob Yukon", Address: "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// UPDATE "items" SET "address"='111 Test Addr',"name"=DEFAULT []
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Bob Yukon' []
}

func ExampleUpdateDataset_Set_withNoTags() {
	type item struct {
		Address string
		Name    string
	}
	sql, args, _ := builder.Update("items").Set(
		item{Name: "Test", Address: "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
}

func ExampleUpdateDataset_Set_withEmbeddedStruct() {
	type Address struct {
		Street string `db:"address_street"`
		State  string `db:"address_state"`
	}
	type User struct {
		Address
		FirstName string
		LastName  string
	}
	ds := builder.Update("user").Set(
		User{Address: Address{Street: "111 Street", State: "NY"}, FirstName: "Greg", LastName: "Farley"},
	)
	updateSQL, args, _ := ds.ToSQL()
	fmt.Println(updateSQL, args)

	// Output:
	// UPDATE "user" SET "address_state"='NY',"address_street"='111 Street',"firstname"='Greg',"lastname"='Farley' []
}

func ExampleUpdateDataset_Set_withIgnoredEmbedded() {
	type Address struct {
		Street string
		State  string
	}
	type User struct {
		Address   `db:"-"`
		FirstName string
		LastName  string
	}
	ds := builder.Update("user").Set(
		User{Address: Address{Street: "111 Street", State: "NY"}, FirstName: "Greg", LastName: "Farley"},
	)
	updateSQL, args, _ := ds.ToSQL()
	fmt.Println(updateSQL, args)

	// Output:
	// UPDATE "user" SET "firstname"='Greg',"lastname"='Farley' []
}

func ExampleUpdateDataset_Set_withNilEmbeddedPointer() {
	type Address struct {
		Street string
		State  string
	}
	type User struct {
		*Address
		FirstName string
		LastName  string
	}
	ds := builder.Update("user").Set(
		User{FirstName: "Greg", LastName: "Farley"},
	)
	updateSQL, args, _ := ds.ToSQL()
	fmt.Println(updateSQL, args)

	// Output:
	// UPDATE "user" SET "firstname"='Greg',"lastname"='Farley' []
}

func ExampleUpdateDataset_ToSQL_prepared() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	sql, args, _ := builder.From("items").Prepared(true).Update().Set(
		item{Name: "Test", Address: "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("items").Prepared(true).Update().Set(
		builder.Record{"name": "Test", "address": "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("items").Prepared(true).Update().Set(
		map[string]any{"name": "Test", "address": "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// UPDATE "items" SET "address"=?,"name"=? [111 Test Addr Test]
	// UPDATE "items" SET "address"=?,"name"=? [111 Test Addr Test]
	// UPDATE "items" SET "address"=?,"name"=? [111 Test Addr Test]
}

func ExampleUpdateDataset_Prepared() {
	sql, args, _ := builder.Update("items").Prepared(true).Set(
		builder.Record{"name": "Test", "address": "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// UPDATE "items" SET "address"=?,"name"=? [111 Test Addr Test]
}
