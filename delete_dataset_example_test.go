package builder_test

import (
	"fmt"

	"github.com/Tooooommy/builder/v9"
	_ "github.com/Tooooommy/builder/v9/dialect/mysql"
)

func ExampleDelete() {
	ds := builder.Delete("items")

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	// Output:
	// DELETE FROM "items" []
}

func ExampleDeleteDataset_Executor() {
	db := getDB()

	de := db.Delete("builder_user").
		Where(builder.Ex{"first_name": "Bob"})
	if r, err := de.Exec(); err != nil {
		fmt.Println(err.Error())
	} else {
		c, _ := r.RowsAffected()
		fmt.Printf("Deleted %d users", c)
	}

	// Output:
	// Deleted 1 users
}

func ExampleDeleteDataset_Executor_returning() {
	db := getDB()

	de := db.Delete("builder_user").
		Where(builder.C("last_name").Eq("Yukon"))

	if ret, err := de.Exec(); err != nil {
		fmt.Println(err.Error())
	} else {
		affect, err := ret.RowsAffected()
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("Deleted users Rows Affected %+v", affect)
	}

	// Output:
	// Deleted users Rows Affected 3
}

func ExampleDeleteDataset_With() {
	sql, _, _ := builder.Delete("test").
		With("check_vals(val)", builder.From().Select(builder.L("123"))).
		Where(builder.C("val").Eq(builder.From("check_vals").Select("val"))).
		ToSQL()
	fmt.Println(sql)

	// Output:
	// WITH check_vals(val) AS (SELECT 123) DELETE FROM "test" WHERE ("val" IN (SELECT "val" FROM "check_vals"))
}

func ExampleDeleteDataset_WithRecursive() {
	sql, _, _ := builder.Delete("nums").
		WithRecursive("nums(x)",
			builder.From().Select(builder.L("1")).
				UnionAll(builder.From("nums").
					Select(builder.L("x+1")).Where(builder.C("x").Lt(5)))).
		ToSQL()
	fmt.Println(sql)
	// Output:
	// WITH RECURSIVE nums(x) AS (SELECT 1 UNION ALL (SELECT x+1 FROM "nums" WHERE ("x" < 5))) DELETE FROM "nums"
}

func ExampleDeleteDataset_Where() {
	// By default everything is anded together
	sql, _, _ := builder.Delete("test").Where(builder.Ex{
		"a": builder.Op{"gt": 10},
		"b": builder.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql)
	// You can use ExOr to get ORed expressions together
	sql, _, _ = builder.Delete("test").Where(builder.ExOr{
		"a": builder.Op{"gt": 10},
		"b": builder.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql)
	// You can use Or with Ex to Or multiple Ex maps together
	sql, _, _ = builder.Delete("test").Where(
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
	sql, _, _ = builder.Delete("test").Where(
		builder.C("a").Gt(10),
		builder.C("b").Lt(10),
		builder.C("c").IsNull(),
		builder.C("d").In("a", "b", "c"),
	).ToSQL()
	fmt.Println(sql)
	// You can use a combination of Ors and Ands
	sql, _, _ = builder.Delete("test").Where(
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
	// DELETE FROM "test" WHERE (("a" > 10) AND ("b" < 10) AND ("c" IS NULL) AND ("d" IN ('a', 'b', 'c')))
	// DELETE FROM "test" WHERE (("a" > 10) OR ("b" < 10) OR ("c" IS NULL) OR ("d" IN ('a', 'b', 'c')))
	// DELETE FROM "test" WHERE ((("a" > 10) AND ("b" < 10)) OR (("c" IS NULL) AND ("d" IN ('a', 'b', 'c'))))
	// DELETE FROM "test" WHERE (("a" > 10) AND ("b" < 10) AND ("c" IS NULL) AND ("d" IN ('a', 'b', 'c')))
	// DELETE FROM "test" WHERE (("a" > 10) OR (("b" < 10) AND ("c" IS NULL)))
}

func ExampleDeleteDataset_Where_prepared() {
	// By default everything is anded together
	sql, args, _ := builder.Delete("test").Prepared(true).Where(builder.Ex{
		"a": builder.Op{"gt": 10},
		"b": builder.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql, args)
	// You can use ExOr to get ORed expressions together
	sql, args, _ = builder.Delete("test").Prepared(true).Where(builder.ExOr{
		"a": builder.Op{"gt": 10},
		"b": builder.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql, args)
	// You can use Or with Ex to Or multiple Ex maps together
	sql, args, _ = builder.Delete("test").Prepared(true).Where(
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
	sql, args, _ = builder.Delete("test").Prepared(true).Where(
		builder.C("a").Gt(10),
		builder.C("b").Lt(10),
		builder.C("c").IsNull(),
		builder.C("d").In("a", "b", "c"),
	).ToSQL()
	fmt.Println(sql, args)
	// You can use a combination of Ors and Ands
	sql, args, _ = builder.Delete("test").Prepared(true).Where(
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
	// DELETE FROM "test" WHERE (("a" > ?) AND ("b" < ?) AND ("c" IS NULL) AND ("d" IN (?, ?, ?))) [10 10 a b c]
	// DELETE FROM "test" WHERE (("a" > ?) OR ("b" < ?) OR ("c" IS NULL) OR ("d" IN (?, ?, ?))) [10 10 a b c]
	// DELETE FROM "test" WHERE ((("a" > ?) AND ("b" < ?)) OR (("c" IS NULL) AND ("d" IN (?, ?, ?)))) [10 10 a b c]
	// DELETE FROM "test" WHERE (("a" > ?) AND ("b" < ?) AND ("c" IS NULL) AND ("d" IN (?, ?, ?))) [10 10 a b c]
	// DELETE FROM "test" WHERE (("a" > ?) OR (("b" < ?) AND ("c" IS NULL))) [10 10]
}

func ExampleDeleteDataset_ClearWhere() {
	ds := builder.Delete("test").Where(
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
	// DELETE FROM "test"
}

func ExampleDeleteDataset_Limit() {
	ds := builder.Dialect("mysql").Delete("test").Limit(10)
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// DELETE FROM `test` LIMIT 10
}

func ExampleDeleteDataset_LimitAll() {
	// Using mysql dialect because it supports limit on delete
	ds := builder.Dialect("mysql").Delete("test").LimitAll()
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// DELETE FROM `test` LIMIT ALL
}

func ExampleDeleteDataset_ClearLimit() {
	// Using mysql dialect because it supports limit on delete
	ds := builder.Dialect("mysql").Delete("test").Limit(10)
	sql, _, _ := ds.ClearLimit().ToSQL()
	fmt.Println(sql)
	// Output:
	// DELETE `test` FROM `test`
}

func ExampleDeleteDataset_Order() {
	// use mysql dialect because it supports order by on deletes
	ds := builder.Dialect("mysql").Delete("test").Order(builder.C("a").Asc())
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// DELETE FROM `test` ORDER BY `a` ASC
}

func ExampleDeleteDataset_OrderAppend() {
	// use mysql dialect because it supports order by on deletes
	ds := builder.Dialect("mysql").Delete("test").Order(builder.C("a").Asc())
	sql, _, _ := ds.OrderAppend(builder.C("b").Desc().NullsLast()).ToSQL()
	fmt.Println(sql)
	// Output:
	// DELETE FROM `test` ORDER BY `a` ASC, `b` DESC NULLS LAST
}

func ExampleDeleteDataset_OrderPrepend() {
	// use mysql dialect because it supports order by on deletes
	ds := builder.Dialect("mysql").Delete("test").Order(builder.C("a").Asc())
	sql, _, _ := ds.OrderPrepend(builder.C("b").Desc().NullsLast()).ToSQL()
	fmt.Println(sql)
	// Output:
	// DELETE FROM `test` ORDER BY `b` DESC NULLS LAST, `a` ASC
}

func ExampleDeleteDataset_ClearOrder() {
	ds := builder.Delete("test").Order(builder.C("a").Asc())
	sql, _, _ := ds.ClearOrder().ToSQL()
	fmt.Println(sql)
	// Output:
	// DELETE FROM "test"
}

func ExampleDeleteDataset_ToSQL() {
	sql, args, _ := builder.Delete("items").ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.Delete("items").
		Where(builder.Ex{"id": builder.Op{"gt": 10}}).
		ToSQL()
	fmt.Println(sql, args)

	// Output:
	// DELETE FROM "items" []
	// DELETE FROM "items" WHERE ("id" > 10) []
}

func ExampleDeleteDataset_Prepared() {
	sql, args, _ := builder.Delete("items").Prepared(true).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.Delete("items").
		Prepared(true).
		Where(builder.Ex{"id": builder.Op{"gt": 10}}).
		ToSQL()
	fmt.Println(sql, args)

	// Output:
	// DELETE FROM "items" []
	// DELETE FROM "items" WHERE ("id" > ?) [10]
}

func ExampleDeleteDataset_Returning() {
	ds := builder.Delete("items")
	sql, args, _ := ds.Returning("id").ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Returning("id").Where(builder.C("id").IsNotNull()).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// DELETE FROM "items" RETURNING "id" []
	// DELETE FROM "items" WHERE ("id" IS NOT NULL) RETURNING "id" []
}
