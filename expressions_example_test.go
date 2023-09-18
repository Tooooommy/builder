// nolint:lll // sql statements are long
package builder_test

import (
	"fmt"
	"regexp"

	"github.com/Tooooommy/builder/v9"
	"github.com/Tooooommy/builder/v9/exp"
)

func ExampleAVG() {
	ds := builder.From("test").Select(builder.AVG("col"))
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT AVG("col") FROM "test" []
	// SELECT AVG("col") FROM "test" []
}

func ExampleAVG_as() {
	sql, _, _ := builder.From("test").Select(builder.AVG("a").As("a")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT AVG("a") AS "a" FROM "test"
}

func ExampleAVG_havingClause() {
	ds := builder.
		From("test").
		Select(builder.AVG("a").As("avg")).
		GroupBy("a").
		Having(builder.AVG("a").Gt(10))

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT AVG("a") AS "avg" FROM "test" GROUP BY "a" HAVING (AVG("a") > 10) []
	// SELECT AVG("a") AS "avg" FROM "test" GROUP BY "a" HAVING (AVG("a") > ?) [10]
}

func ExampleAnd() {
	ds := builder.From("test").Where(
		builder.And(
			builder.C("col").Gt(10),
			builder.C("col").Lt(20),
		),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE (("col" > 10) AND ("col" < 20)) []
	// SELECT * FROM "test" WHERE (("col" > ?) AND ("col" < ?)) [10 20]
}

// You can use And with Or to create more complex queries
func ExampleAnd_withOr() {
	ds := builder.From("test").Where(
		builder.And(
			builder.C("col1").IsTrue(),
			builder.Or(
				builder.C("col2").Gt(10),
				builder.C("col2").Lt(20),
			),
		),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// by default expressions are anded together
	ds = builder.From("test").Where(
		builder.C("col1").IsTrue(),
		builder.Or(
			builder.C("col2").Gt(10),
			builder.C("col2").Lt(20),
		),
	)
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE (("col1" IS TRUE) AND (("col2" > 10) OR ("col2" < 20))) []
	// SELECT * FROM "test" WHERE (("col1" IS TRUE) AND (("col2" > ?) OR ("col2" < ?))) [10 20]
	// SELECT * FROM "test" WHERE (("col1" IS TRUE) AND (("col2" > 10) OR ("col2" < 20))) []
	// SELECT * FROM "test" WHERE (("col1" IS TRUE) AND (("col2" > ?) OR ("col2" < ?))) [10 20]
}

// You can use ExOr inside of And expression lists.
func ExampleAnd_withExOr() {
	// by default expressions are anded together
	ds := builder.From("test").Where(
		builder.C("col1").IsTrue(),
		builder.ExOr{
			"col2": builder.Op{"gt": 10},
			"col3": builder.Op{"lt": 20},
		},
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE (("col1" IS TRUE) AND (("col2" > 10) OR ("col3" < 20))) []
	// SELECT * FROM "test" WHERE (("col1" IS TRUE) AND (("col2" > ?) OR ("col3" < ?))) [10 20]
}

func ExampleC() {
	sql, args, _ := builder.From("test").
		Select(builder.C("*")).
		ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").
		Select(builder.C("col1")).
		ToSQL()
	fmt.Println(sql, args)

	ds := builder.From("test").Where(
		builder.C("col1").Eq(10),
		builder.C("col2").In([]int64{1, 2, 3, 4}),
		builder.C("col3").Like(regexp.MustCompile("^[ab]")),
		builder.C("col4").IsNull(),
	)

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" []
	// SELECT "col1" FROM "test" []
	// SELECT * FROM "test" WHERE (("col1" = 10) AND ("col2" IN (1, 2, 3, 4)) AND ("col3" ~ '^[ab]') AND ("col4" IS NULL)) []
	// SELECT * FROM "test" WHERE (("col1" = ?) AND ("col2" IN (?, ?, ?, ?)) AND ("col3" ~ ?) AND ("col4" IS NULL)) [10 1 2 3 4 ^[ab]]
}

func ExampleC_as() {
	sql, _, _ := builder.From("test").Select(builder.C("a").As("as_a")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Select(builder.C("a").As(builder.C("as_a"))).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT "a" AS "as_a" FROM "test"
	// SELECT "a" AS "as_a" FROM "test"
}

func ExampleC_ordering() {
	sql, args, _ := builder.From("test").Order(builder.C("a").Asc()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Order(builder.C("a").Asc().NullsFirst()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Order(builder.C("a").Asc().NullsLast()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Order(builder.C("a").Desc()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Order(builder.C("a").Desc().NullsFirst()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Order(builder.C("a").Desc().NullsLast()).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC []
	// SELECT * FROM "test" ORDER BY "a" ASC NULLS FIRST []
	// SELECT * FROM "test" ORDER BY "a" ASC NULLS LAST []
	// SELECT * FROM "test" ORDER BY "a" DESC []
	// SELECT * FROM "test" ORDER BY "a" DESC NULLS FIRST []
	// SELECT * FROM "test" ORDER BY "a" DESC NULLS LAST []
}

func ExampleC_cast() {
	sql, _, _ := builder.From("test").
		Select(builder.C("json1").Cast("TEXT").As("json_text")).
		ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(
		builder.C("json1").Cast("TEXT").Neq(
			builder.C("json2").Cast("TEXT"),
		),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT CAST("json1" AS TEXT) AS "json_text" FROM "test"
	// SELECT * FROM "test" WHERE (CAST("json1" AS TEXT) != CAST("json2" AS TEXT))
}

func ExampleC_comparisons() {
	// used from an identifier
	sql, _, _ := builder.From("test").Where(builder.C("a").Eq(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.C("a").Neq(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.C("a").Gt(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.C("a").Gte(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.C("a").Lt(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.C("a").Lte(10)).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "test" WHERE ("a" = 10)
	// SELECT * FROM "test" WHERE ("a" != 10)
	// SELECT * FROM "test" WHERE ("a" > 10)
	// SELECT * FROM "test" WHERE ("a" >= 10)
	// SELECT * FROM "test" WHERE ("a" < 10)
	// SELECT * FROM "test" WHERE ("a" <= 10)
}

func ExampleC_inOperators() {
	// using identifiers
	sql, _, _ := builder.From("test").Where(builder.C("a").In("a", "b", "c")).ToSQL()
	fmt.Println(sql)
	// with a slice
	sql, _, _ = builder.From("test").Where(builder.C("a").In([]string{"a", "b", "c"})).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.C("a").NotIn("a", "b", "c")).ToSQL()
	fmt.Println(sql)
	// with a slice
	sql, _, _ = builder.From("test").Where(builder.C("a").NotIn([]string{"a", "b", "c"})).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" NOT IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" NOT IN ('a', 'b', 'c'))
}

func ExampleC_likeComparisons() {
	// using identifiers
	sql, _, _ := builder.From("test").Where(builder.C("a").Like("%a%")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.C("a").Like(regexp.MustCompile(`[ab]`))).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.C("a").ILike("%a%")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.C("a").ILike(regexp.MustCompile("[ab]"))).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.C("a").NotLike("%a%")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.C("a").NotLike(regexp.MustCompile("[ab]"))).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.C("a").NotILike("%a%")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.C("a").NotILike(regexp.MustCompile(`[ab]`))).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "test" WHERE ("a" LIKE '%a%')
	// SELECT * FROM "test" WHERE ("a" ~ '[ab]')
	// SELECT * FROM "test" WHERE ("a" ILIKE '%a%')
	// SELECT * FROM "test" WHERE ("a" ~* '[ab]')
	// SELECT * FROM "test" WHERE ("a" NOT LIKE '%a%')
	// SELECT * FROM "test" WHERE ("a" !~ '[ab]')
	// SELECT * FROM "test" WHERE ("a" NOT ILIKE '%a%')
	// SELECT * FROM "test" WHERE ("a" !~* '[ab]')
}

func ExampleC_isComparisons() {
	sql, args, _ := builder.From("test").Where(builder.C("a").Is(nil)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.C("a").Is(true)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.C("a").Is(false)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.C("a").IsNull()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.C("a").IsTrue()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.C("a").IsFalse()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.C("a").IsNot(nil)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.C("a").IsNot(true)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.C("a").IsNot(false)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.C("a").IsNotNull()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.C("a").IsNotTrue()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.C("a").IsNotFalse()).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("a" IS NULL) []
	// SELECT * FROM "test" WHERE ("a" IS TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS FALSE) []
	// SELECT * FROM "test" WHERE ("a" IS NULL) []
	// SELECT * FROM "test" WHERE ("a" IS TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS FALSE) []
	// SELECT * FROM "test" WHERE ("a" IS NOT NULL) []
	// SELECT * FROM "test" WHERE ("a" IS NOT TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS NOT FALSE) []
	// SELECT * FROM "test" WHERE ("a" IS NOT NULL) []
	// SELECT * FROM "test" WHERE ("a" IS NOT TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS NOT FALSE) []
}

func ExampleC_betweenComparisons() {
	ds := builder.From("test").Where(
		builder.C("a").Between(builder.Range(1, 10)),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(
		builder.C("a").NotBetween(builder.Range(1, 10)),
	)
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("a" BETWEEN 1 AND 10) []
	// SELECT * FROM "test" WHERE ("a" BETWEEN ? AND ?) [1 10]
	// SELECT * FROM "test" WHERE ("a" NOT BETWEEN 1 AND 10) []
	// SELECT * FROM "test" WHERE ("a" NOT BETWEEN ? AND ?) [1 10]
}

func ExampleCOALESCE() {
	ds := builder.From("test").Select(
		builder.COALESCE(builder.C("a"), "a"),
		builder.COALESCE(builder.C("a"), builder.C("b"), nil),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT COALESCE("a", 'a'), COALESCE("a", "b", NULL) FROM "test" []
	// SELECT COALESCE("a", ?), COALESCE("a", "b", ?) FROM "test" [a <nil>]
}

func ExampleCOALESCE_as() {
	sql, _, _ := builder.From("test").Select(builder.COALESCE(builder.C("a"), "a").As("a")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT COALESCE("a", 'a') AS "a" FROM "test"
}

func ExampleCOUNT() {
	ds := builder.From("test").Select(builder.COUNT("*"))
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT COUNT(*) FROM "test" []
	// SELECT COUNT(*) FROM "test" []
}

func ExampleCOUNT_as() {
	sql, _, _ := builder.From("test").Select(builder.COUNT("*").As("count")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT COUNT(*) AS "count" FROM "test"
}

func ExampleCOUNT_havingClause() {
	ds := builder.
		From("test").
		Select(builder.COUNT("a").As("COUNT")).
		GroupBy("a").
		Having(builder.COUNT("a").Gt(10))

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT COUNT("a") AS "COUNT" FROM "test" GROUP BY "a" HAVING (COUNT("a") > 10) []
	// SELECT COUNT("a") AS "COUNT" FROM "test" GROUP BY "a" HAVING (COUNT("a") > ?) [10]
}

func ExampleCast() {
	sql, _, _ := builder.From("test").
		Select(builder.Cast(builder.C("json1"), "TEXT").As("json_text")).
		ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(
		builder.Cast(builder.C("json1"), "TEXT").Neq(
			builder.Cast(builder.C("json2"), "TEXT"),
		),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT CAST("json1" AS TEXT) AS "json_text" FROM "test"
	// SELECT * FROM "test" WHERE (CAST("json1" AS TEXT) != CAST("json2" AS TEXT))
}

func ExampleDISTINCT() {
	ds := builder.From("test").Select(builder.DISTINCT("col"))
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT DISTINCT("col") FROM "test" []
	// SELECT DISTINCT("col") FROM "test" []
}

func ExampleDISTINCT_as() {
	sql, _, _ := builder.From("test").Select(builder.DISTINCT("a").As("distinct_a")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT DISTINCT("a") AS "distinct_a" FROM "test"
}

func ExampleDefault() {
	ds := builder.Insert("items")

	sql, args, _ := ds.Rows(builder.Record{
		"name":    builder.Default(),
		"address": builder.Default(),
	}).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).Rows(builder.Record{
		"name":    builder.Default(),
		"address": builder.Default(),
	}).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT) []
	// INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT) []
}

func ExampleDoNothing() {
	ds := builder.Insert("items")

	sql, args, _ := ds.Rows(builder.Record{
		"address": "111 Address",
		"name":    "bob",
	}).OnConflict(builder.DoNothing()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).Rows(builder.Record{
		"address": "111 Address",
		"name":    "bob",
	}).OnConflict(builder.DoNothing()).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Address', 'bob') ON CONFLICT DO NOTHING []
	// INSERT INTO "items" ("address", "name") VALUES (?, ?) ON CONFLICT DO NOTHING [111 Address bob]
}

func ExampleDoUpdate() {
	ds := builder.Insert("items")

	sql, args, _ := ds.
		Rows(builder.Record{"address": "111 Address"}).
		OnConflict(builder.DoUpdate("address", builder.C("address").Set(builder.I("excluded.address")))).
		ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).
		Rows(builder.Record{"address": "111 Address"}).
		OnConflict(builder.DoUpdate("address", builder.C("address").Set(builder.I("excluded.address")))).
		ToSQL()
	fmt.Println(sql, args)

	// Output:
	// INSERT INTO "items" ("address") VALUES ('111 Address') ON CONFLICT (address) DO UPDATE SET "address"="excluded"."address" []
	// INSERT INTO "items" ("address") VALUES (?) ON CONFLICT (address) DO UPDATE SET "address"="excluded"."address" [111 Address]
}

func ExampleDoUpdate_where() {
	ds := builder.Insert("items")

	sql, args, _ := ds.
		Rows(builder.Record{"address": "111 Address"}).
		OnConflict(builder.DoUpdate(
			"address",
			builder.C("address").Set(builder.I("excluded.address"))).Where(builder.I("items.updated").IsNull()),
		).
		ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).
		Rows(builder.Record{"address": "111 Address"}).
		OnConflict(builder.DoUpdate(
			"address",
			builder.C("address").Set(builder.I("excluded.address"))).Where(builder.I("items.updated").IsNull()),
		).
		ToSQL()
	fmt.Println(sql, args)

	// Output:
	// INSERT INTO "items" ("address") VALUES ('111 Address') ON CONFLICT (address) DO UPDATE SET "address"="excluded"."address" WHERE ("items"."updated" IS NULL) []
	// INSERT INTO "items" ("address") VALUES (?) ON CONFLICT (address) DO UPDATE SET "address"="excluded"."address" WHERE ("items"."updated" IS NULL) [111 Address]
}

func ExampleFIRST() {
	ds := builder.From("test").Select(builder.FIRST("col"))
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT FIRST("col") FROM "test" []
	// SELECT FIRST("col") FROM "test" []
}

func ExampleFIRST_as() {
	sql, _, _ := builder.From("test").Select(builder.FIRST("a").As("a")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT FIRST("a") AS "a" FROM "test"
}

// This example shows how to create custom SQL Functions
func ExampleFunc() {
	stragg := func(expression exp.Expression, delimiter string) exp.SQLFunctionExpression {
		return builder.Func("str_agg", expression, builder.L(delimiter))
	}
	sql, _, _ := builder.From("test").Select(stragg(builder.C("col"), "|")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT str_agg("col", |) FROM "test"
}

func ExampleI() {
	ds := builder.From("test").
		Select(
			builder.I("my_schema.table.col1"),
			builder.I("table.col2"),
			builder.I("col3"),
		)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Select(builder.I("test.*"))

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT "my_schema"."table"."col1", "table"."col2", "col3" FROM "test" []
	// SELECT "my_schema"."table"."col1", "table"."col2", "col3" FROM "test" []
	// SELECT "test".* FROM "test" []
	// SELECT "test".* FROM "test" []
}

func ExampleL() {
	ds := builder.From("test").Where(
		// literal with no args
		builder.L(`"col"::TEXT = ""other_col"::text`),
		// literal with args they will be interpolated into the sql by default
		builder.L("col IN (?, ?, ?)", "a", "b", "c"),
	)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "test" WHERE ("col"::TEXT = ""other_col"::text AND col IN ('a', 'b', 'c')) []
	// SELECT * FROM "test" WHERE ("col"::TEXT = ""other_col"::text AND col IN (?, ?, ?)) [a b c]
}

func ExampleL_withArgs() {
	ds := builder.From("test").Where(
		builder.L(
			"(? AND ?) OR ?",
			builder.C("a").Eq(1),
			builder.C("b").Eq("b"),
			builder.C("c").In([]string{"a", "b", "c"}),
		),
	)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "test" WHERE (("a" = 1) AND ("b" = 'b')) OR ("c" IN ('a', 'b', 'c')) []
	// SELECT * FROM "test" WHERE (("a" = ?) AND ("b" = ?)) OR ("c" IN (?, ?, ?)) [1 b a b c]
}

func ExampleL_as() {
	sql, _, _ := builder.From("test").Select(builder.L("json_col->>'totalAmount'").As("total_amount")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT json_col->>'totalAmount' AS "total_amount" FROM "test"
}

func ExampleL_comparisons() {
	// used from a literal expression
	sql, _, _ := builder.From("test").Where(builder.L("(a + b)").Eq(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.L("(a + b)").Neq(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.L("(a + b)").Gt(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.L("(a + b)").Gte(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.L("(a + b)").Lt(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.L("(a + b)").Lte(10)).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "test" WHERE ((a + b) = 10)
	// SELECT * FROM "test" WHERE ((a + b) != 10)
	// SELECT * FROM "test" WHERE ((a + b) > 10)
	// SELECT * FROM "test" WHERE ((a + b) >= 10)
	// SELECT * FROM "test" WHERE ((a + b) < 10)
	// SELECT * FROM "test" WHERE ((a + b) <= 10)
}

func ExampleL_inOperators() {
	// using identifiers
	sql, _, _ := builder.From("test").Where(builder.L("json_col->>'val'").In("a", "b", "c")).ToSQL()
	fmt.Println(sql)
	// with a slice
	sql, _, _ = builder.From("test").Where(builder.L("json_col->>'val'").In([]string{"a", "b", "c"})).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.L("json_col->>'val'").NotIn("a", "b", "c")).ToSQL()
	fmt.Println(sql)
	// with a slice
	sql, _, _ = builder.From("test").Where(builder.L("json_col->>'val'").NotIn([]string{"a", "b", "c"})).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "test" WHERE (json_col->>'val' IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE (json_col->>'val' IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE (json_col->>'val' NOT IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE (json_col->>'val' NOT IN ('a', 'b', 'c'))
}

func ExampleL_likeComparisons() {
	// using identifiers
	sql, _, _ := builder.From("test").Where(builder.L("(a::text || 'bar')").Like("%a%")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(
		builder.L("(a::text || 'bar')").Like(regexp.MustCompile("[ab]")),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.L("(a::text || 'bar')").ILike("%a%")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(
		builder.L("(a::text || 'bar')").ILike(regexp.MustCompile("[ab]")),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.L("(a::text || 'bar')").NotLike("%a%")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(
		builder.L("(a::text || 'bar')").NotLike(regexp.MustCompile("[ab]")),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(builder.L("(a::text || 'bar')").NotILike("%a%")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Where(
		builder.L("(a::text || 'bar')").NotILike(regexp.MustCompile("[ab]")),
	).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "test" WHERE ((a::text || 'bar') LIKE '%a%')
	// SELECT * FROM "test" WHERE ((a::text || 'bar') ~ '[ab]')
	// SELECT * FROM "test" WHERE ((a::text || 'bar') ILIKE '%a%')
	// SELECT * FROM "test" WHERE ((a::text || 'bar') ~* '[ab]')
	// SELECT * FROM "test" WHERE ((a::text || 'bar') NOT LIKE '%a%')
	// SELECT * FROM "test" WHERE ((a::text || 'bar') !~ '[ab]')
	// SELECT * FROM "test" WHERE ((a::text || 'bar') NOT ILIKE '%a%')
	// SELECT * FROM "test" WHERE ((a::text || 'bar') !~* '[ab]')
}

func ExampleL_isComparisons() {
	sql, args, _ := builder.From("test").Where(builder.L("a").Is(nil)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.L("a").Is(true)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.L("a").Is(false)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.L("a").IsNull()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.L("a").IsTrue()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.L("a").IsFalse()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.L("a").IsNot(nil)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.L("a").IsNot(true)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.L("a").IsNot(false)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.L("a").IsNotNull()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.L("a").IsNotTrue()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("test").Where(builder.L("a").IsNotFalse()).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE (a IS NULL) []
	// SELECT * FROM "test" WHERE (a IS TRUE) []
	// SELECT * FROM "test" WHERE (a IS FALSE) []
	// SELECT * FROM "test" WHERE (a IS NULL) []
	// SELECT * FROM "test" WHERE (a IS TRUE) []
	// SELECT * FROM "test" WHERE (a IS FALSE) []
	// SELECT * FROM "test" WHERE (a IS NOT NULL) []
	// SELECT * FROM "test" WHERE (a IS NOT TRUE) []
	// SELECT * FROM "test" WHERE (a IS NOT FALSE) []
	// SELECT * FROM "test" WHERE (a IS NOT NULL) []
	// SELECT * FROM "test" WHERE (a IS NOT TRUE) []
	// SELECT * FROM "test" WHERE (a IS NOT FALSE) []
}

func ExampleL_betweenComparisons() {
	ds := builder.From("test").Where(
		builder.L("(a + b)").Between(builder.Range(1, 10)),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(
		builder.L("(a + b)").NotBetween(builder.Range(1, 10)),
	)
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ((a + b) BETWEEN 1 AND 10) []
	// SELECT * FROM "test" WHERE ((a + b) BETWEEN ? AND ?) [1 10]
	// SELECT * FROM "test" WHERE ((a + b) NOT BETWEEN 1 AND 10) []
	// SELECT * FROM "test" WHERE ((a + b) NOT BETWEEN ? AND ?) [1 10]
}

func ExampleLAST() {
	ds := builder.From("test").Select(builder.LAST("col"))
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT LAST("col") FROM "test" []
	// SELECT LAST("col") FROM "test" []
}

func ExampleLAST_as() {
	sql, _, _ := builder.From("test").Select(builder.LAST("a").As("a")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT LAST("a") AS "a" FROM "test"
}

func ExampleMAX() {
	ds := builder.From("test").Select(builder.MAX("col"))
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT MAX("col") FROM "test" []
	// SELECT MAX("col") FROM "test" []
}

func ExampleMAX_as() {
	sql, _, _ := builder.From("test").Select(builder.MAX("a").As("a")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT MAX("a") AS "a" FROM "test"
}

func ExampleMAX_havingClause() {
	ds := builder.
		From("test").
		Select(builder.MAX("a").As("MAX")).
		GroupBy("a").
		Having(builder.MAX("a").Gt(10))

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT MAX("a") AS "MAX" FROM "test" GROUP BY "a" HAVING (MAX("a") > 10) []
	// SELECT MAX("a") AS "MAX" FROM "test" GROUP BY "a" HAVING (MAX("a") > ?) [10]
}

func ExampleMIN() {
	ds := builder.From("test").Select(builder.MIN("col"))
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT MIN("col") FROM "test" []
	// SELECT MIN("col") FROM "test" []
}

func ExampleMIN_as() {
	sql, _, _ := builder.From("test").Select(builder.MIN("a").As("a")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT MIN("a") AS "a" FROM "test"
}

func ExampleMIN_havingClause() {
	ds := builder.
		From("test").
		Select(builder.MIN("a").As("MIN")).
		GroupBy("a").
		Having(builder.MIN("a").Gt(10))

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT MIN("a") AS "MIN" FROM "test" GROUP BY "a" HAVING (MIN("a") > 10) []
	// SELECT MIN("a") AS "MIN" FROM "test" GROUP BY "a" HAVING (MIN("a") > ?) [10]
}

func ExampleOn() {
	ds := builder.From("test").Join(
		builder.T("my_table"),
		builder.On(builder.I("my_table.fkey").Eq(builder.I("other_table.id"))),
	)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "test" INNER JOIN "my_table" ON ("my_table"."fkey" = "other_table"."id") []
	// SELECT * FROM "test" INNER JOIN "my_table" ON ("my_table"."fkey" = "other_table"."id") []
}

func ExampleOn_withEx() {
	ds := builder.From("test").Join(
		builder.T("my_table"),
		builder.On(builder.Ex{"my_table.fkey": builder.I("other_table.id")}),
	)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "test" INNER JOIN "my_table" ON ("my_table"."fkey" = "other_table"."id") []
	// SELECT * FROM "test" INNER JOIN "my_table" ON ("my_table"."fkey" = "other_table"."id") []
}

func ExampleOr() {
	ds := builder.From("test").Where(
		builder.Or(
			builder.C("col").Eq(10),
			builder.C("col").Eq(20),
		),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE (("col" = 10) OR ("col" = 20)) []
	// SELECT * FROM "test" WHERE (("col" = ?) OR ("col" = ?)) [10 20]
}

func ExampleOr_withAnd() {
	ds := builder.From("items").Where(
		builder.Or(
			builder.C("a").Gt(10),
			builder.And(
				builder.C("b").Eq(100),
				builder.C("c").Neq("test"),
			),
		),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "items" WHERE (("a" > 10) OR (("b" = 100) AND ("c" != 'test'))) []
	// SELECT * FROM "items" WHERE (("a" > ?) OR (("b" = ?) AND ("c" != ?))) [10 100 test]
}

func ExampleOr_withExMap() {
	ds := builder.From("test").Where(
		builder.Or(
			// Ex will be anded together
			builder.Ex{
				"col1": 1,
				"col2": true,
			},
			builder.Ex{
				"col3": nil,
				"col4": "foo",
			},
		),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ((("col1" = 1) AND ("col2" IS TRUE)) OR (("col3" IS NULL) AND ("col4" = 'foo'))) []
	// SELECT * FROM "test" WHERE ((("col1" = ?) AND ("col2" IS TRUE)) OR (("col3" IS NULL) AND ("col4" = ?))) [1 foo]
}

func ExampleRange_numbers() {
	ds := builder.From("test").Where(
		builder.C("col").Between(builder.Range(1, 10)),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(
		builder.C("col").NotBetween(builder.Range(1, 10)),
	)
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("col" BETWEEN 1 AND 10) []
	// SELECT * FROM "test" WHERE ("col" BETWEEN ? AND ?) [1 10]
	// SELECT * FROM "test" WHERE ("col" NOT BETWEEN 1 AND 10) []
	// SELECT * FROM "test" WHERE ("col" NOT BETWEEN ? AND ?) [1 10]
}

func ExampleRange_strings() {
	ds := builder.From("test").Where(
		builder.C("col").Between(builder.Range("a", "z")),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(
		builder.C("col").NotBetween(builder.Range("a", "z")),
	)
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("col" BETWEEN 'a' AND 'z') []
	// SELECT * FROM "test" WHERE ("col" BETWEEN ? AND ?) [a z]
	// SELECT * FROM "test" WHERE ("col" NOT BETWEEN 'a' AND 'z') []
	// SELECT * FROM "test" WHERE ("col" NOT BETWEEN ? AND ?) [a z]
}

func ExampleRange_identifiers() {
	ds := builder.From("test").Where(
		builder.C("col1").Between(builder.Range(builder.C("col2"), builder.C("col3"))),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(
		builder.C("col1").NotBetween(builder.Range(builder.C("col2"), builder.C("col3"))),
	)
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("col1" BETWEEN "col2" AND "col3") []
	// SELECT * FROM "test" WHERE ("col1" BETWEEN "col2" AND "col3") []
	// SELECT * FROM "test" WHERE ("col1" NOT BETWEEN "col2" AND "col3") []
	// SELECT * FROM "test" WHERE ("col1" NOT BETWEEN "col2" AND "col3") []
}

func ExampleS() {
	s := builder.S("test_schema")
	t := s.Table("test")
	sql, args, _ := builder.
		From(t).
		Select(
			t.Col("col1"),
			t.Col("col2"),
			t.Col("col3"),
		).
		ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT "test_schema"."test"."col1", "test_schema"."test"."col2", "test_schema"."test"."col3" FROM "test_schema"."test" []
}

func ExampleSUM() {
	ds := builder.From("test").Select(builder.SUM("col"))
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT SUM("col") FROM "test" []
	// SELECT SUM("col") FROM "test" []
}

func ExampleSUM_as() {
	sql, _, _ := builder.From("test").Select(builder.SUM("a").As("a")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT SUM("a") AS "a" FROM "test"
}

func ExampleSUM_havingClause() {
	ds := builder.
		From("test").
		Select(builder.SUM("a").As("SUM")).
		GroupBy("a").
		Having(builder.SUM("a").Gt(10))

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT SUM("a") AS "SUM" FROM "test" GROUP BY "a" HAVING (SUM("a") > 10) []
	// SELECT SUM("a") AS "SUM" FROM "test" GROUP BY "a" HAVING (SUM("a") > ?) [10]
}

func ExampleStar() {
	ds := builder.From("test").Select(builder.Star())

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" []
	// SELECT * FROM "test" []
}

func ExampleT() {
	t := builder.T("test")
	sql, args, _ := builder.
		From(t).
		Select(
			t.Col("col1"),
			t.Col("col2"),
			t.Col("col3"),
		).
		ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT "test"."col1", "test"."col2", "test"."col3" FROM "test" []
}

func ExampleUsing() {
	ds := builder.From("test").Join(
		builder.T("my_table"),
		builder.Using("fkey"),
	)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "test" INNER JOIN "my_table" USING ("fkey") []
	// SELECT * FROM "test" INNER JOIN "my_table" USING ("fkey") []
}

func ExampleUsing_withIdentifier() {
	ds := builder.From("test").Join(
		builder.T("my_table"),
		builder.Using(builder.C("fkey")),
	)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "test" INNER JOIN "my_table" USING ("fkey") []
	// SELECT * FROM "test" INNER JOIN "my_table" USING ("fkey") []
}

func ExampleEx() {
	ds := builder.From("items").Where(
		builder.Ex{
			"col1": "a",
			"col2": 1,
			"col3": true,
			"col4": false,
			"col5": nil,
			"col6": []string{"a", "b", "c"},
		},
	)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "items" WHERE (("col1" = 'a') AND ("col2" = 1) AND ("col3" IS TRUE) AND ("col4" IS FALSE) AND ("col5" IS NULL) AND ("col6" IN ('a', 'b', 'c'))) []
	// SELECT * FROM "items" WHERE (("col1" = ?) AND ("col2" = ?) AND ("col3" IS TRUE) AND ("col4" IS FALSE) AND ("col5" IS NULL) AND ("col6" IN (?, ?, ?))) [a 1 a b c]
}

func ExampleEx_withOp() {
	sql, args, _ := builder.From("items").Where(
		builder.Ex{
			"col1": builder.Op{"neq": "a"},
			"col3": builder.Op{"isNot": true},
			"col6": builder.Op{"notIn": []string{"a", "b", "c"}},
		},
	).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "items" WHERE (("col1" != 'a') AND ("col3" IS NOT TRUE) AND ("col6" NOT IN ('a', 'b', 'c'))) []
}

func ExampleEx_in() {
	// using an Ex expression map
	sql, _, _ := builder.From("test").Where(builder.Ex{
		"a": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c'))
}

func ExampleExOr() {
	sql, args, _ := builder.From("items").Where(
		builder.ExOr{
			"col1": "a",
			"col2": 1,
			"col3": true,
			"col4": false,
			"col5": nil,
			"col6": []string{"a", "b", "c"},
		},
	).ToSQL()
	fmt.Println(sql, args)

	// nolint:lll // sql statements are long
	// Output:
	// SELECT * FROM "items" WHERE (("col1" = 'a') OR ("col2" = 1) OR ("col3" IS TRUE) OR ("col4" IS FALSE) OR ("col5" IS NULL) OR ("col6" IN ('a', 'b', 'c'))) []
}

func ExampleExOr_withOp() {
	sql, _, _ := builder.From("items").Where(builder.ExOr{
		"col1": builder.Op{"neq": "a"},
		"col3": builder.Op{"isNot": true},
		"col6": builder.Op{"notIn": []string{"a", "b", "c"}},
	}).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("items").Where(builder.ExOr{
		"col1": builder.Op{"gt": 1},
		"col2": builder.Op{"gte": 1},
		"col3": builder.Op{"lt": 1},
		"col4": builder.Op{"lte": 1},
	}).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("items").Where(builder.ExOr{
		"col1": builder.Op{"like": "a%"},
		"col2": builder.Op{"notLike": "a%"},
		"col3": builder.Op{"iLike": "a%"},
		"col4": builder.Op{"notILike": "a%"},
	}).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("items").Where(builder.ExOr{
		"col1": builder.Op{"like": regexp.MustCompile("^[ab]")},
		"col2": builder.Op{"notLike": regexp.MustCompile("^[ab]")},
		"col3": builder.Op{"iLike": regexp.MustCompile("^[ab]")},
		"col4": builder.Op{"notILike": regexp.MustCompile("^[ab]")},
	}).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "items" WHERE (("col1" != 'a') OR ("col3" IS NOT TRUE) OR ("col6" NOT IN ('a', 'b', 'c')))
	// SELECT * FROM "items" WHERE (("col1" > 1) OR ("col2" >= 1) OR ("col3" < 1) OR ("col4" <= 1))
	// SELECT * FROM "items" WHERE (("col1" LIKE 'a%') OR ("col2" NOT LIKE 'a%') OR ("col3" ILIKE 'a%') OR ("col4" NOT ILIKE 'a%'))
	// SELECT * FROM "items" WHERE (("col1" ~ '^[ab]') OR ("col2" !~ '^[ab]') OR ("col3" ~* '^[ab]') OR ("col4" !~* '^[ab]'))
}

func ExampleOp_comparisons() {
	ds := builder.From("test").Where(builder.Ex{
		"a": 10,
		"b": builder.Op{"neq": 10},
		"c": builder.Op{"gte": 10},
		"d": builder.Op{"lt": 10},
		"e": builder.Op{"lte": 10},
	})

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE (("a" = 10) AND ("b" != 10) AND ("c" >= 10) AND ("d" < 10) AND ("e" <= 10)) []
	// SELECT * FROM "test" WHERE (("a" = ?) AND ("b" != ?) AND ("c" >= ?) AND ("d" < ?) AND ("e" <= ?)) [10 10 10 10 10]
}

func ExampleOp_inComparisons() {
	// using an Ex expression map
	ds := builder.From("test").Where(builder.Ex{
		"a": builder.Op{"in": []string{"a", "b", "c"}},
	})

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": builder.Op{"notIn": []string{"a", "b", "c"}},
	})

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c')) []
	// SELECT * FROM "test" WHERE ("a" IN (?, ?, ?)) [a b c]
	// SELECT * FROM "test" WHERE ("a" NOT IN ('a', 'b', 'c')) []
	// SELECT * FROM "test" WHERE ("a" NOT IN (?, ?, ?)) [a b c]
}

func ExampleOp_likeComparisons() {
	// using an Ex expression map
	ds := builder.From("test").Where(builder.Ex{
		"a": builder.Op{"like": "%a%"},
	})
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": builder.Op{"like": regexp.MustCompile("[ab]")},
	})

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": builder.Op{"iLike": "%a%"},
	})

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": builder.Op{"iLike": regexp.MustCompile("[ab]")},
	})

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": builder.Op{"notLike": "%a%"},
	})

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": builder.Op{"notLike": regexp.MustCompile("[ab]")},
	})

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": builder.Op{"notILike": "%a%"},
	})

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": builder.Op{"notILike": regexp.MustCompile("[ab]")},
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("a" LIKE '%a%') []
	// SELECT * FROM "test" WHERE ("a" LIKE ?) [%a%]
	// SELECT * FROM "test" WHERE ("a" ~ '[ab]') []
	// SELECT * FROM "test" WHERE ("a" ~ ?) [[ab]]
	// SELECT * FROM "test" WHERE ("a" ILIKE '%a%') []
	// SELECT * FROM "test" WHERE ("a" ILIKE ?) [%a%]
	// SELECT * FROM "test" WHERE ("a" ~* '[ab]') []
	// SELECT * FROM "test" WHERE ("a" ~* ?) [[ab]]
	// SELECT * FROM "test" WHERE ("a" NOT LIKE '%a%') []
	// SELECT * FROM "test" WHERE ("a" NOT LIKE ?) [%a%]
	// SELECT * FROM "test" WHERE ("a" !~ '[ab]') []
	// SELECT * FROM "test" WHERE ("a" !~ ?) [[ab]]
	// SELECT * FROM "test" WHERE ("a" NOT ILIKE '%a%') []
	// SELECT * FROM "test" WHERE ("a" NOT ILIKE ?) [%a%]
	// SELECT * FROM "test" WHERE ("a" !~* '[ab]') []
	// SELECT * FROM "test" WHERE ("a" !~* ?) [[ab]]
}

func ExampleOp_isComparisons() {
	// using an Ex expression map
	ds := builder.From("test").Where(builder.Ex{
		"a": true,
	})
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": builder.Op{"is": true},
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": false,
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": builder.Op{"is": false},
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": nil,
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": builder.Op{"is": nil},
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": builder.Op{"isNot": true},
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": builder.Op{"isNot": false},
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": builder.Op{"isNot": nil},
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("a" IS TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS FALSE) []
	// SELECT * FROM "test" WHERE ("a" IS FALSE) []
	// SELECT * FROM "test" WHERE ("a" IS FALSE) []
	// SELECT * FROM "test" WHERE ("a" IS FALSE) []
	// SELECT * FROM "test" WHERE ("a" IS NULL) []
	// SELECT * FROM "test" WHERE ("a" IS NULL) []
	// SELECT * FROM "test" WHERE ("a" IS NULL) []
	// SELECT * FROM "test" WHERE ("a" IS NULL) []
	// SELECT * FROM "test" WHERE ("a" IS NOT TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS NOT TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS NOT FALSE) []
	// SELECT * FROM "test" WHERE ("a" IS NOT FALSE) []
	// SELECT * FROM "test" WHERE ("a" IS NOT NULL) []
	// SELECT * FROM "test" WHERE ("a" IS NOT NULL) []
}

func ExampleOp_betweenComparisons() {
	ds := builder.From("test").Where(builder.Ex{
		"a": builder.Op{"between": builder.Range(1, 10)},
	})
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("test").Where(builder.Ex{
		"a": builder.Op{"notBetween": builder.Range(1, 10)},
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("a" BETWEEN 1 AND 10) []
	// SELECT * FROM "test" WHERE ("a" BETWEEN ? AND ?) [1 10]
	// SELECT * FROM "test" WHERE ("a" NOT BETWEEN 1 AND 10) []
	// SELECT * FROM "test" WHERE ("a" NOT BETWEEN ? AND ?) [1 10]
}

// When using a single op with multiple keys they are ORed together
func ExampleOp_withMultipleKeys() {
	ds := builder.From("items").Where(builder.Ex{
		"col1": builder.Op{"is": nil, "eq": 10},
	})

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "items" WHERE (("col1" = 10) OR ("col1" IS NULL)) []
	// SELECT * FROM "items" WHERE (("col1" = ?) OR ("col1" IS NULL)) [10]
}

func ExampleRecord_insert() {
	ds := builder.Insert("test")

	records := []builder.Record{
		{"col1": 1, "col2": "foo"},
		{"col1": 2, "col2": "bar"},
	}

	sql, args, _ := ds.Rows(records).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).Rows(records).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// INSERT INTO "test" ("col1", "col2") VALUES (1, 'foo'), (2, 'bar') []
	// INSERT INTO "test" ("col1", "col2") VALUES (?, ?), (?, ?) [1 foo 2 bar]
}

func ExampleRecord_update() {
	ds := builder.Update("test")
	update := builder.Record{"col1": 1, "col2": "foo"}

	sql, args, _ := ds.Set(update).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).Set(update).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// UPDATE "test" SET "col1"=1,"col2"='foo' []
	// UPDATE "test" SET "col1"=?,"col2"=? [1 foo]
}

func ExampleV() {
	ds := builder.From("user").Select(
		builder.V(true).As("is_verified"),
		builder.V(1.2).As("version"),
		"first_name",
		"last_name",
	)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("user").Where(builder.V(1).Neq(1))
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT TRUE AS "is_verified", 1.2 AS "version", "first_name", "last_name" FROM "user" []
	// SELECT * FROM "user" WHERE (1 != 1) []
}

func ExampleV_prepared() {
	ds := builder.From("user").Select(
		builder.V(true).As("is_verified"),
		builder.V(1.2).As("version"),
		"first_name",
		"last_name",
	)

	sql, args, _ := ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = builder.From("user").Where(builder.V(1).Neq(1))

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT ? AS "is_verified", ? AS "version", "first_name", "last_name" FROM "user" [true 1.2]
	// SELECT * FROM "user" WHERE (? != ?) [1 1]
}

func ExampleVals() {
	ds := builder.Insert("user").
		Cols("first_name", "last_name", "is_verified").
		Vals(
			builder.Vals{"Greg", "Farley", true},
			builder.Vals{"Jimmy", "Stewart", true},
			builder.Vals{"Jeff", "Jeffers", false},
		)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("first_name", "last_name", "is_verified") VALUES ('Greg', 'Farley', TRUE), ('Jimmy', 'Stewart', TRUE), ('Jeff', 'Jeffers', FALSE) []
}

func ExampleW() {
	ds := builder.From("test").
		Select(builder.ROW_NUMBER().Over(builder.W().PartitionBy("a").OrderBy(builder.I("b").Asc())))
	query, args, _ := ds.ToSQL()
	fmt.Println(query, args)

	ds = builder.From("test").
		Select(builder.ROW_NUMBER().OverName(builder.I("w"))).
		Window(builder.W("w").PartitionBy("a").OrderBy(builder.I("b").Asc()))
	query, args, _ = ds.ToSQL()
	fmt.Println(query, args)

	ds = builder.From("test").
		Select(builder.ROW_NUMBER().OverName(builder.I("w1"))).
		Window(
			builder.W("w1").PartitionBy("a"),
			builder.W("w").Inherit("w1").OrderBy(builder.I("b").Asc()),
		)
	query, args, _ = ds.ToSQL()
	fmt.Println(query, args)

	ds = builder.From("test").
		Select(builder.ROW_NUMBER().Over(builder.W().Inherit("w").OrderBy("b"))).
		Window(builder.W("w").PartitionBy("a"))
	query, args, _ = ds.ToSQL()
	fmt.Println(query, args)
	// Output:
	// SELECT ROW_NUMBER() OVER (PARTITION BY "a" ORDER BY "b" ASC) FROM "test" []
	// SELECT ROW_NUMBER() OVER "w" FROM "test" WINDOW "w" AS (PARTITION BY "a" ORDER BY "b" ASC) []
	// SELECT ROW_NUMBER() OVER "w1" FROM "test" WINDOW "w1" AS (PARTITION BY "a"), "w" AS ("w1" ORDER BY "b" ASC) []
	// SELECT ROW_NUMBER() OVER ("w" ORDER BY "b") FROM "test" WINDOW "w" AS (PARTITION BY "a") []
}

func ExampleLateral() {
	maxEntry := builder.From("entry").
		Select(builder.MAX("int").As("max_int")).
		Where(builder.Ex{"time": builder.Op{"lt": builder.I("e.time")}}).
		As("max_entry")

	maxID := builder.From("entry").
		Select("id").
		Where(builder.Ex{"int": builder.I("max_entry.max_int")}).
		As("max_id")

	ds := builder.
		Select("e.id", "max_entry.max_int", "max_id.id").
		From(
			builder.T("entry").As("e"),
			builder.Lateral(maxEntry),
			builder.Lateral(maxID),
		)
	query, args, _ := ds.ToSQL()
	fmt.Println(query, args)

	query, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(query, args)

	// Output:
	// SELECT "e"."id", "max_entry"."max_int", "max_id"."id" FROM "entry" AS "e", LATERAL (SELECT MAX("int") AS "max_int" FROM "entry" WHERE ("time" < "e"."time")) AS "max_entry", LATERAL (SELECT "id" FROM "entry" WHERE ("int" = "max_entry"."max_int")) AS "max_id" []
	// SELECT "e"."id", "max_entry"."max_int", "max_id"."id" FROM "entry" AS "e", LATERAL (SELECT MAX("int") AS "max_int" FROM "entry" WHERE ("time" < "e"."time")) AS "max_entry", LATERAL (SELECT "id" FROM "entry" WHERE ("int" = "max_entry"."max_int")) AS "max_id" []
}

func ExampleLateral_join() {
	maxEntry := builder.From("entry").
		Select(builder.MAX("int").As("max_int")).
		Where(builder.Ex{"time": builder.Op{"lt": builder.I("e.time")}}).
		As("max_entry")

	maxID := builder.From("entry").
		Select("id").
		Where(builder.Ex{"int": builder.I("max_entry.max_int")}).
		As("max_id")

	ds := builder.
		Select("e.id", "max_entry.max_int", "max_id.id").
		From(builder.T("entry").As("e")).
		Join(builder.Lateral(maxEntry), builder.On(builder.V(true))).
		Join(builder.Lateral(maxID), builder.On(builder.V(true)))
	query, args, _ := ds.ToSQL()
	fmt.Println(query, args)

	query, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(query, args)

	// Output:
	// SELECT "e"."id", "max_entry"."max_int", "max_id"."id" FROM "entry" AS "e" INNER JOIN LATERAL (SELECT MAX("int") AS "max_int" FROM "entry" WHERE ("time" < "e"."time")) AS "max_entry" ON TRUE INNER JOIN LATERAL (SELECT "id" FROM "entry" WHERE ("int" = "max_entry"."max_int")) AS "max_id" ON TRUE []
	// SELECT "e"."id", "max_entry"."max_int", "max_id"."id" FROM "entry" AS "e" INNER JOIN LATERAL (SELECT MAX("int") AS "max_int" FROM "entry" WHERE ("time" < "e"."time")) AS "max_entry" ON ? INNER JOIN LATERAL (SELECT "id" FROM "entry" WHERE ("int" = "max_entry"."max_int")) AS "max_id" ON ? [true true]
}

func ExampleAny() {
	ds := builder.From("test").Where(builder.Ex{
		"id": builder.Any(builder.From("other").Select("test_id")),
	})
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "test" WHERE ("id" = ANY ((SELECT "test_id" FROM "other"))) []
	// SELECT * FROM "test" WHERE ("id" = ANY ((SELECT "test_id" FROM "other"))) []
}

func ExampleAll() {
	ds := builder.From("test").Where(builder.Ex{
		"id": builder.All(builder.From("other").Select("test_id")),
	})
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "test" WHERE ("id" = ALL ((SELECT "test_id" FROM "other"))) []
	// SELECT * FROM "test" WHERE ("id" = ALL ((SELECT "test_id" FROM "other"))) []
}

func ExampleCase_search() {
	ds := builder.From("test").
		Select(
			builder.C("col"),
			builder.Case().
				When(builder.C("col").Gt(0), true).
				When(builder.C("col").Lte(0), false).
				As("is_gt_zero"),
		)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT "col", CASE  WHEN ("col" > 0) THEN TRUE WHEN ("col" <= 0) THEN FALSE END AS "is_gt_zero" FROM "test" []
	// SELECT "col", CASE  WHEN ("col" > ?) THEN ? WHEN ("col" <= ?) THEN ? END AS "is_gt_zero" FROM "test" [0 true 0 false]
}

func ExampleCase_searchElse() {
	ds := builder.From("test").
		Select(
			builder.C("col"),
			builder.Case().
				When(builder.C("col").Gt(10), "Gt 10").
				When(builder.C("col").Gt(20), "Gt 20").
				Else("Bad Val").
				As("str_val"),
		)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT "col", CASE  WHEN ("col" > 10) THEN 'Gt 10' WHEN ("col" > 20) THEN 'Gt 20' ELSE 'Bad Val' END AS "str_val" FROM "test" []
	// SELECT "col", CASE  WHEN ("col" > ?) THEN ? WHEN ("col" > ?) THEN ? ELSE ? END AS "str_val" FROM "test" [10 Gt 10 20 Gt 20 Bad Val]
}

func ExampleCase_value() {
	ds := builder.From("test").
		Select(
			builder.C("col"),
			builder.Case().
				Value(builder.C("str")).
				When("foo", "FOO").
				When("bar", "BAR").
				As("foo_bar_upper"),
		)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT "col", CASE "str" WHEN 'foo' THEN 'FOO' WHEN 'bar' THEN 'BAR' END AS "foo_bar_upper" FROM "test" []
	// SELECT "col", CASE "str" WHEN ? THEN ? WHEN ? THEN ? END AS "foo_bar_upper" FROM "test" [foo FOO bar BAR]
}

func ExampleCase_valueElse() {
	ds := builder.From("test").
		Select(
			builder.C("col"),
			builder.Case().
				Value(builder.C("str")).
				When("foo", "FOO").
				When("bar", "BAR").
				Else("Baz").
				As("foo_bar_upper"),
		)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT "col", CASE "str" WHEN 'foo' THEN 'FOO' WHEN 'bar' THEN 'BAR' ELSE 'Baz' END AS "foo_bar_upper" FROM "test" []
	// SELECT "col", CASE "str" WHEN ? THEN ? WHEN ? THEN ? ELSE ? END AS "foo_bar_upper" FROM "test" [foo FOO bar BAR Baz]
}
