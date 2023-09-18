// nolint:lll // sql statements are long
package builder_test

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/Tooooommy/builder/v9"
	"github.com/Tooooommy/builder/v9/exp"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

const table = `
		DROP TABLE IF EXISTS user_role;
		DROP TABLE IF EXISTS builder_user;	
		CREATE TABLE builder_user (
    		id bigint AUTO_INCREMENT,
			first_name varchar(45) NOT NULL,
			last_name VARCHAR(45) NOT NULL,
			created timestamp DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (id)
		) ENGINE = InnoDB COLLATE utf8mb4_general_ci;
		CREATE TABLE user_role (
    		id bigint AUTO_INCREMENT,
			user_id bigint DEFAULT 0,
			name varchar(45) NOT NULL,
			created timestamp DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (id)
		) ENGINE = InnoDB COLLATE utf8mb4_general_ci; 
    `

var (
	dropUserRoleTable = `
		DROP TABLE IF EXISTS user_role;
`
	dropBuilderUserTable = `
		DROP TABLE IF EXISTS builder_user;
`

	createBuilderUserTable = `
		CREATE TABLE builder_user (
    		id bigint AUTO_INCREMENT,
			first_name varchar(45) NOT NULL,
			last_name VARCHAR(45) NOT NULL,
			created timestamp DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (id)
		) ENGINE = InnoDB COLLATE utf8mb4_general_ci;
`

	createUserRoleTable = `
		CREATE TABLE user_role (
    		id bigint AUTO_INCREMENT,
			user_id bigint DEFAULT 0,
			name varchar(45) NOT NULL,
			created timestamp DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (id)
		) ENGINE = InnoDB COLLATE utf8mb4_general_ci;
`

	tables = []string{dropUserRoleTable, dropBuilderUserTable, createBuilderUserTable, createUserRoleTable}
)

const defaultDBURI = "tommy:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true"

var builderDB *builder.Database

func getDB() *builder.Database {
	if builderDB == nil {
		dbURI := os.Getenv("MY_URI")
		if dbURI == "" {
			dbURI = defaultDBURI
		}
		conn := sqlx.NewSqlConn("mysql", dbURI)
		builderDB = builder.New("mysql", conn)
	}
	// reset the db
	for _, table := range tables {
		if _, err := builderDB.Exec(table); err != nil {
			panic(err)
		}
	}

	type builderUser struct {
		ID        int64     `db:"id" builder:"skipinsert"`
		FirstName string    `db:"first_name"`
		LastName  string    `db:"last_name"`
		Created   time.Time `db:"created" builder:"skipupdate,skipinsert"`
	}

	type userRole struct {
		ID      int64     `db:"id" builder:"skipinsert"`
		UserID  int64     `db:"user_id"`
		Name    string    `db:"name"`
		Created time.Time `db:"created" builder:"skipupdate,skipinsert"`
	}

	users := []builderUser{
		{FirstName: "Bob", LastName: "Yukon"},
		{FirstName: "Sally", LastName: "Yukon"},
		{FirstName: "Vinita", LastName: "Yukon"},
		{FirstName: "John", LastName: "Doe"},
	}

	roles := []userRole{
		{Name: "Admin"},
		{Name: "Manager"},
		{Name: "Manager"},
		{Name: "User"},
	}
	for index, user := range users {
		ret, err := builderDB.Insert("builder_user").Rows(user).Exec()
		if err != nil {
			panic(err)
		}
		roles[index].UserID, err = ret.LastInsertId()
		if err != nil {
			panic(err)
		}
	}
	_, err := builderDB.Insert("user_role").Rows(roles).Exec()
	if err != nil {
		panic(err)
	}
	return builderDB
}

func ExampleSelectDataset() {
	ds := builder.From("test").
		Select(builder.COUNT("*")).
		InnerJoin(builder.T("test2"), builder.On(builder.I("test.fkey").Eq(builder.I("test2.id")))).
		LeftJoin(builder.T("test3"), builder.On(builder.I("test2.fkey").Eq(builder.I("test3.id")))).
		Where(
			builder.Ex{
				"test.name": builder.Op{
					"like": regexp.MustCompile("^[ab]"),
				},
				"test2.amount": builder.Op{
					"isNot": nil,
				},
			},
			builder.ExOr{
				"test3.id":     nil,
				"test3.status": []string{"passed", "active", "registered"},
			}).
		Order(builder.I("test.created").Desc().NullsLast()).
		GroupBy(builder.I("test.user_id")).
		Having(builder.AVG("test3.age").Gt(10))

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// nolint:lll // SQL statements are long
	// Output:
	// SELECT COUNT(*) FROM "test" INNER JOIN "test2" ON ("test"."fkey" = "test2"."id") LEFT JOIN "test3" ON ("test2"."fkey" = "test3"."id") WHERE ((("test"."name" ~ '^[ab]') AND ("test2"."amount" IS NOT NULL)) AND (("test3"."id" IS NULL) OR ("test3"."status" IN ('passed', 'active', 'registered')))) GROUP BY "test"."user_id" HAVING (AVG("test3"."age") > 10) ORDER BY "test"."created" DESC NULLS LAST []
	// SELECT COUNT(*) FROM "test" INNER JOIN "test2" ON ("test"."fkey" = "test2"."id") LEFT JOIN "test3" ON ("test2"."fkey" = "test3"."id") WHERE ((("test"."name" ~ ?) AND ("test2"."amount" IS NOT NULL)) AND (("test3"."id" IS NULL) OR ("test3"."status" IN (?, ?, ?)))) GROUP BY "test"."user_id" HAVING (AVG("test3"."age") > ?) ORDER BY "test"."created" DESC NULLS LAST [^[ab] passed active registered 10]
}

func ExampleSelect() {
	sql, _, _ := builder.Select(builder.L("NOW()")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT NOW()
}

func ExampleFrom() {
	sql, args, _ := builder.From("test").ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" []
}

func ExampleSelectDataset_As() {
	ds := builder.From("test").As("t")
	sql, _, _ := builder.From(ds).ToSQL()
	fmt.Println(sql)
	// Output: SELECT * FROM (SELECT * FROM "test") AS "t"
}

func ExampleSelectDataset_Union() {
	sql, _, _ := builder.From("test").
		Union(builder.From("test2")).
		ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").
		Limit(1).
		Union(builder.From("test2")).
		ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").
		Limit(1).
		Union(builder.From("test2").
			Order(builder.C("id").Desc())).
		ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" UNION (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleSelectDataset_UnionAll() {
	sql, _, _ := builder.From("test").
		UnionAll(builder.From("test2")).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = builder.From("test").
		Limit(1).
		UnionAll(builder.From("test2")).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = builder.From("test").
		Limit(1).
		UnionAll(builder.From("test2").
			Order(builder.C("id").Desc())).
		ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" UNION ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION ALL (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleSelectDataset_With() {
	sql, _, _ := builder.From("one").
		With("one", builder.From().Select(builder.L("1"))).
		Select(builder.Star()).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = builder.From("derived").
		With("intermed", builder.From("test").Select(builder.Star()).Where(builder.C("x").Gte(5))).
		With("derived", builder.From("intermed").Select(builder.Star()).Where(builder.C("x").Lt(10))).
		Select(builder.Star()).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = builder.From("multi").
		With("multi(x,y)", builder.From().Select(builder.L("1"), builder.L("2"))).
		Select(builder.C("x"), builder.C("y")).
		ToSQL()
	fmt.Println(sql)

	// Output:
	// WITH one AS (SELECT 1) SELECT * FROM "one"
	// WITH intermed AS (SELECT * FROM "test" WHERE ("x" >= 5)), derived AS (SELECT * FROM "intermed" WHERE ("x" < 10)) SELECT * FROM "derived"
	// WITH multi(x,y) AS (SELECT 1, 2) SELECT "x", "y" FROM "multi"
}

func ExampleSelectDataset_With_insertDataset() {
	insertDs := builder.Insert("foo").Rows(builder.Record{"user_id": 10}).Returning("id")

	ds := builder.From("bar").
		With("ins", insertDs).
		Select("bar_name").
		Where(builder.Ex{"bar.user_id": builder.I("ins.user_id")})

	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)

	sql, args, _ := ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// WITH ins AS (INSERT INTO "foo" ("user_id") VALUES (10) RETURNING "id") SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "ins"."user_id")
	// WITH ins AS (INSERT INTO "foo" ("user_id") VALUES (?) RETURNING "id") SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "ins"."user_id") [10]
}

func ExampleSelectDataset_With_updateDataset() {
	updateDs := builder.Update("foo").Set(builder.Record{"bar": "baz"}).Returning("id")

	ds := builder.From("bar").
		With("upd", updateDs).
		Select("bar_name").
		Where(builder.Ex{"bar.user_id": builder.I("upd.user_id")})

	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)

	sql, args, _ := ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// WITH upd AS (UPDATE "foo" SET "bar"='baz' RETURNING "id") SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "upd"."user_id")
	// WITH upd AS (UPDATE "foo" SET "bar"=? RETURNING "id") SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "upd"."user_id") [baz]
}

func ExampleSelectDataset_With_deleteDataset() {
	deleteDs := builder.Delete("foo").Where(builder.Ex{"bar": "baz"}).Returning("id")

	ds := builder.From("bar").
		With("del", deleteDs).
		Select("bar_name").
		Where(builder.Ex{"bar.user_id": builder.I("del.user_id")})

	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)

	sql, args, _ := ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// WITH del AS (DELETE FROM "foo" WHERE ("bar" = 'baz') RETURNING "id") SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "del"."user_id")
	// WITH del AS (DELETE FROM "foo" WHERE ("bar" = ?) RETURNING "id") SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "del"."user_id") [baz]
}

func ExampleSelectDataset_WithRecursive() {
	sql, _, _ := builder.From("nums").
		WithRecursive("nums(x)",
			builder.From().Select(builder.L("1")).
				UnionAll(builder.From("nums").
					Select(builder.L("x+1")).Where(builder.C("x").Lt(5)))).
		ToSQL()
	fmt.Println(sql)
	// Output:
	// WITH RECURSIVE nums(x) AS (SELECT 1 UNION ALL (SELECT x+1 FROM "nums" WHERE ("x" < 5))) SELECT * FROM "nums"
}

func ExampleSelectDataset_Intersect() {
	sql, _, _ := builder.From("test").
		Intersect(builder.From("test2")).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = builder.From("test").
		Limit(1).
		Intersect(builder.From("test2")).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = builder.From("test").
		Limit(1).
		Intersect(builder.From("test2").
			Order(builder.C("id").Desc())).
		ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INTERSECT (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleSelectDataset_IntersectAll() {
	sql, _, _ := builder.From("test").
		IntersectAll(builder.From("test2")).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = builder.From("test").
		Limit(1).
		IntersectAll(builder.From("test2")).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = builder.From("test").
		Limit(1).
		IntersectAll(builder.From("test2").
			Order(builder.C("id").Desc())).
		ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INTERSECT ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT ALL (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleSelectDataset_ClearOffset() {
	ds := builder.From("test").
		Offset(2)
	sql, _, _ := ds.
		ClearOffset().
		ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleSelectDataset_Offset() {
	ds := builder.From("test").Offset(2)
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" OFFSET 2
}

func ExampleSelectDataset_Limit() {
	ds := builder.From("test").Limit(10)
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LIMIT 10
}

func ExampleSelectDataset_LimitAll() {
	ds := builder.From("test").LimitAll()
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LIMIT ALL
}

func ExampleSelectDataset_ClearLimit() {
	ds := builder.From("test").Limit(10)
	sql, _, _ := ds.ClearLimit().ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleSelectDataset_Order() {
	ds := builder.From("test").Order(builder.C("a").Asc())
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC
}

func ExampleSelectDataset_Order_caseExpression() {
	ds := builder.From("test").Order(builder.Case().When(builder.C("num").Gt(10), 0).Else(1).Asc())
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY CASE  WHEN ("num" > 10) THEN 0 ELSE 1 END ASC
}

func ExampleSelectDataset_OrderAppend() {
	ds := builder.From("test").Order(builder.C("a").Asc())
	sql, _, _ := ds.OrderAppend(builder.C("b").Desc().NullsLast()).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC, "b" DESC NULLS LAST
}

func ExampleSelectDataset_OrderPrepend() {
	ds := builder.From("test").Order(builder.C("a").Asc())
	sql, _, _ := ds.OrderPrepend(builder.C("b").Desc().NullsLast()).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "b" DESC NULLS LAST, "a" ASC
}

func ExampleSelectDataset_ClearOrder() {
	ds := builder.From("test").Order(builder.C("a").Asc())
	sql, _, _ := ds.ClearOrder().ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleSelectDataset_GroupBy() {
	sql, _, _ := builder.From("test").
		Select(builder.SUM("income").As("income_sum")).
		GroupBy("age").
		ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT SUM("income") AS "income_sum" FROM "test" GROUP BY "age"
}

func ExampleSelectDataset_GroupByAppend() {
	ds := builder.From("test").
		Select(builder.SUM("income").As("income_sum")).
		GroupBy("age")
	sql, _, _ := ds.
		GroupByAppend("job").
		ToSQL()
	fmt.Println(sql)
	// the original dataset group by does not change
	sql, _, _ = ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT SUM("income") AS "income_sum" FROM "test" GROUP BY "age", "job"
	// SELECT SUM("income") AS "income_sum" FROM "test" GROUP BY "age"
}

func ExampleSelectDataset_Having() {
	sql, _, _ := builder.From("test").Having(builder.SUM("income").Gt(1000)).ToSQL()
	fmt.Println(sql)
	sql, _, _ = builder.From("test").GroupBy("age").Having(builder.SUM("income").Gt(1000)).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" HAVING (SUM("income") > 1000)
	// SELECT * FROM "test" GROUP BY "age" HAVING (SUM("income") > 1000)
}

func ExampleSelectDataset_Window() {
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
	// Output
	// SELECT ROW_NUMBER() OVER (PARTITION BY "a" ORDER BY "b" ASC) FROM "test" []
	// SELECT ROW_NUMBER() OVER "w" FROM "test" WINDOW "w" AS (PARTITION BY "a" ORDER BY "b" ASC) []
	// SELECT ROW_NUMBER() OVER "w" FROM "test" WINDOW "w1" AS (PARTITION BY "a"), "w" AS ("w1" ORDER BY "b" ASC) []
	// SELECT ROW_NUMBER() OVER ("w" ORDER BY "b") FROM "test" WINDOW "w" AS (PARTITION BY "a") []
}

func ExampleSelectDataset_Where() {
	// By default everything is anded together
	sql, _, _ := builder.From("test").Where(builder.Ex{
		"a": builder.Op{"gt": 10},
		"b": builder.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql)
	// You can use ExOr to get ORed expressions together
	sql, _, _ = builder.From("test").Where(builder.ExOr{
		"a": builder.Op{"gt": 10},
		"b": builder.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql)
	// You can use Or with Ex to Or multiple Ex maps together
	sql, _, _ = builder.From("test").Where(
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
	sql, _, _ = builder.From("test").Where(
		builder.C("a").Gt(10),
		builder.C("b").Lt(10),
		builder.C("c").IsNull(),
		builder.C("d").In("a", "b", "c"),
	).ToSQL()
	fmt.Println(sql)
	// You can use a combination of Ors and Ands
	sql, _, _ = builder.From("test").Where(
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
	// SELECT * FROM "test" WHERE (("a" > 10) AND ("b" < 10) AND ("c" IS NULL) AND ("d" IN ('a', 'b', 'c')))
	// SELECT * FROM "test" WHERE (("a" > 10) OR ("b" < 10) OR ("c" IS NULL) OR ("d" IN ('a', 'b', 'c')))
	// SELECT * FROM "test" WHERE ((("a" > 10) AND ("b" < 10)) OR (("c" IS NULL) AND ("d" IN ('a', 'b', 'c'))))
	// SELECT * FROM "test" WHERE (("a" > 10) AND ("b" < 10) AND ("c" IS NULL) AND ("d" IN ('a', 'b', 'c')))
	// SELECT * FROM "test" WHERE (("a" > 10) OR (("b" < 10) AND ("c" IS NULL)))
}

func ExampleSelectDataset_Where_prepared() {
	// By default everything is anded together
	sql, args, _ := builder.From("test").Prepared(true).Where(builder.Ex{
		"a": builder.Op{"gt": 10},
		"b": builder.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql, args)
	// You can use ExOr to get ORed expressions together
	sql, args, _ = builder.From("test").Prepared(true).Where(builder.ExOr{
		"a": builder.Op{"gt": 10},
		"b": builder.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql, args)
	// You can use Or with Ex to Or multiple Ex maps together
	sql, args, _ = builder.From("test").Prepared(true).Where(
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
	sql, args, _ = builder.From("test").Prepared(true).Where(
		builder.C("a").Gt(10),
		builder.C("b").Lt(10),
		builder.C("c").IsNull(),
		builder.C("d").In("a", "b", "c"),
	).ToSQL()
	fmt.Println(sql, args)
	// You can use a combination of Ors and Ands
	sql, args, _ = builder.From("test").Prepared(true).Where(
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
	// SELECT * FROM "test" WHERE (("a" > ?) AND ("b" < ?) AND ("c" IS NULL) AND ("d" IN (?, ?, ?))) [10 10 a b c]
	// SELECT * FROM "test" WHERE (("a" > ?) OR ("b" < ?) OR ("c" IS NULL) OR ("d" IN (?, ?, ?))) [10 10 a b c]
	// SELECT * FROM "test" WHERE ((("a" > ?) AND ("b" < ?)) OR (("c" IS NULL) AND ("d" IN (?, ?, ?)))) [10 10 a b c]
	// SELECT * FROM "test" WHERE (("a" > ?) AND ("b" < ?) AND ("c" IS NULL) AND ("d" IN (?, ?, ?))) [10 10 a b c]
	// SELECT * FROM "test" WHERE (("a" > ?) OR (("b" < ?) AND ("c" IS NULL))) [10 10]
}

func ExampleSelectDataset_ClearWhere() {
	ds := builder.From("test").Where(
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
	// SELECT * FROM "test"
}

func ExampleSelectDataset_Join() {
	sql, _, _ := builder.From("test").Join(
		builder.T("test2"),
		builder.On(builder.Ex{"test.fkey": builder.I("test2.Id")}),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Join(builder.T("test2"), builder.Using("common_column")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Join(
		builder.From("test2").Where(builder.C("amount").Gt(0)),
		builder.On(builder.I("test.fkey").Eq(builder.T("test2").Col("Id"))),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").Join(
		builder.From("test2").Where(builder.C("amount").Gt(0)).As("t"),
		builder.On(builder.T("test").Col("fkey").Eq(builder.T("t").Col("Id"))),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INNER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleSelectDataset_InnerJoin() {
	sql, _, _ := builder.From("test").InnerJoin(
		builder.T("test2"),
		builder.On(builder.Ex{
			"test.fkey": builder.I("test2.Id"),
		}),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").InnerJoin(
		builder.T("test2"),
		builder.Using("common_column"),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").InnerJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)),
		builder.On(builder.I("test.fkey").Eq(builder.I("test2.Id"))),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").InnerJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)).As("t"),
		builder.On(builder.I("test.fkey").Eq(builder.I("t.Id"))),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INNER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleSelectDataset_FullOuterJoin() {
	sql, _, _ := builder.From("test").FullOuterJoin(
		builder.T("test2"),
		builder.On(builder.Ex{
			"test.fkey": builder.I("test2.Id"),
		}),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").FullOuterJoin(
		builder.T("test2"),
		builder.Using("common_column"),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").FullOuterJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)),
		builder.On(builder.I("test.fkey").Eq(builder.I("test2.Id"))),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").FullOuterJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)).As("t"),
		builder.On(builder.I("test.fkey").Eq(builder.I("t.Id"))),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" FULL OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL OUTER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" FULL OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleSelectDataset_RightOuterJoin() {
	sql, _, _ := builder.From("test").RightOuterJoin(
		builder.T("test2"),
		builder.On(builder.Ex{
			"test.fkey": builder.I("test2.Id"),
		}),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").RightOuterJoin(
		builder.T("test2"),
		builder.Using("common_column"),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").RightOuterJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)),
		builder.On(builder.I("test.fkey").Eq(builder.I("test2.Id"))),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").RightOuterJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)).As("t"),
		builder.On(builder.I("test.fkey").Eq(builder.I("t.Id"))),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" RIGHT OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT OUTER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" RIGHT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleSelectDataset_LeftOuterJoin() {
	sql, _, _ := builder.From("test").LeftOuterJoin(
		builder.T("test2"),
		builder.On(builder.Ex{
			"test.fkey": builder.I("test2.Id"),
		}),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").LeftOuterJoin(
		builder.T("test2"),
		builder.Using("common_column"),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").LeftOuterJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)),
		builder.On(builder.I("test.fkey").Eq(builder.I("test2.Id"))),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").LeftOuterJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)).As("t"),
		builder.On(builder.I("test.fkey").Eq(builder.I("t.Id"))),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LEFT OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT OUTER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" LEFT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleSelectDataset_FullJoin() {
	sql, _, _ := builder.From("test").FullJoin(
		builder.T("test2"),
		builder.On(builder.Ex{
			"test.fkey": builder.I("test2.Id"),
		}),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").FullJoin(
		builder.T("test2"),
		builder.Using("common_column"),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").FullJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)),
		builder.On(builder.I("test.fkey").Eq(builder.I("test2.Id"))),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").FullJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)).As("t"),
		builder.On(builder.I("test.fkey").Eq(builder.I("t.Id"))),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" FULL JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleSelectDataset_RightJoin() {
	sql, _, _ := builder.From("test").RightJoin(
		builder.T("test2"),
		builder.On(builder.Ex{
			"test.fkey": builder.I("test2.Id"),
		}),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").RightJoin(
		builder.T("test2"),
		builder.Using("common_column"),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").RightJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)),
		builder.On(builder.I("test.fkey").Eq(builder.I("test2.Id"))),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").RightJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)).As("t"),
		builder.On(builder.I("test.fkey").Eq(builder.I("t.Id"))),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" RIGHT JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleSelectDataset_LeftJoin() {
	sql, _, _ := builder.From("test").LeftJoin(
		builder.T("test2"),
		builder.On(builder.Ex{
			"test.fkey": builder.I("test2.Id"),
		}),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").LeftJoin(
		builder.T("test2"),
		builder.Using("common_column"),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").LeftJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)),
		builder.On(builder.I("test.fkey").Eq(builder.I("test2.Id"))),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").LeftJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)).As("t"),
		builder.On(builder.I("test.fkey").Eq(builder.I("t.Id"))),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LEFT JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleSelectDataset_NaturalJoin() {
	sql, _, _ := builder.From("test").NaturalJoin(builder.T("test2")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").NaturalJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").NaturalJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)).As("t"),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL JOIN "test2"
	// SELECT * FROM "test" NATURAL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleSelectDataset_NaturalLeftJoin() {
	sql, _, _ := builder.From("test").NaturalLeftJoin(builder.T("test2")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").NaturalLeftJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").NaturalLeftJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)).As("t"),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL LEFT JOIN "test2"
	// SELECT * FROM "test" NATURAL LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleSelectDataset_NaturalRightJoin() {
	sql, _, _ := builder.From("test").NaturalRightJoin(builder.T("test2")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").NaturalRightJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").NaturalRightJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)).As("t"),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL RIGHT JOIN "test2"
	// SELECT * FROM "test" NATURAL RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleSelectDataset_NaturalFullJoin() {
	sql, _, _ := builder.From("test").NaturalFullJoin(builder.T("test2")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").NaturalFullJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").NaturalFullJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)).As("t"),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL FULL JOIN "test2"
	// SELECT * FROM "test" NATURAL FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleSelectDataset_CrossJoin() {
	sql, _, _ := builder.From("test").CrossJoin(builder.T("test2")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").CrossJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = builder.From("test").CrossJoin(
		builder.From("test2").Where(builder.C("amount").Gt(0)).As("t"),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" CROSS JOIN "test2"
	// SELECT * FROM "test" CROSS JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" CROSS JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleSelectDataset_FromSelf() {
	sql, _, _ := builder.From("test").FromSelf().ToSQL()
	fmt.Println(sql)
	sql, _, _ = builder.From("test").As("my_test_table").FromSelf().ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test") AS "t1"
	// SELECT * FROM (SELECT * FROM "test") AS "my_test_table"
}

func ExampleSelectDataset_From() {
	ds := builder.From("test")
	sql, _, _ := ds.From("test2").ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test2"
}

func ExampleSelectDataset_From_withDataset() {
	ds := builder.From("test")
	fromDs := ds.Where(builder.C("age").Gt(10))
	sql, _, _ := ds.From(fromDs).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test" WHERE ("age" > 10)) AS "t1"
}

func ExampleSelectDataset_From_withAliasedDataset() {
	ds := builder.From("test")
	fromDs := ds.Where(builder.C("age").Gt(10))
	sql, _, _ := ds.From(fromDs.As("test2")).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test" WHERE ("age" > 10)) AS "test2"
}

func ExampleSelectDataset_Select() {
	sql, _, _ := builder.From("test").Select("a", "b", "c").ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT "a", "b", "c" FROM "test"
}

func ExampleSelectDataset_Select_withDataset() {
	ds := builder.From("test")
	fromDs := ds.Select("age").Where(builder.C("age").Gt(10))
	sql, _, _ := ds.From().Select(fromDs).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT (SELECT "age" FROM "test" WHERE ("age" > 10))
}

func ExampleSelectDataset_Select_withAliasedDataset() {
	ds := builder.From("test")
	fromDs := ds.Select("age").Where(builder.C("age").Gt(10))
	sql, _, _ := ds.From().Select(fromDs.As("ages")).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT (SELECT "age" FROM "test" WHERE ("age" > 10)) AS "ages"
}

func ExampleSelectDataset_Select_withLiteral() {
	sql, _, _ := builder.From("test").Select(builder.L("a + b").As("sum")).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT a + b AS "sum" FROM "test"
}

func ExampleSelectDataset_Select_withSQLFunctionExpression() {
	sql, _, _ := builder.From("test").Select(
		builder.COUNT("*").As("age_count"),
		builder.MAX("age").As("max_age"),
		builder.AVG("age").As("avg_age"),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT COUNT(*) AS "age_count", MAX("age") AS "max_age", AVG("age") AS "avg_age" FROM "test"
}

func ExampleSelectDataset_Select_withStruct() {
	ds := builder.From("test")

	type myStruct struct {
		Name         string
		Address      string `db:"address"`
		EmailAddress string `db:"email_address"`
	}

	// Pass with pointer
	sql, _, _ := ds.Select(&myStruct{}).ToSQL()
	fmt.Println(sql)

	// Pass instance of
	sql, _, _ = ds.Select(myStruct{}).ToSQL()
	fmt.Println(sql)

	type myStruct2 struct {
		myStruct
		Zipcode string `db:"zipcode"`
	}

	// Pass pointer to struct with embedded struct
	sql, _, _ = ds.Select(&myStruct2{}).ToSQL()
	fmt.Println(sql)

	// Pass instance of struct with embedded struct
	sql, _, _ = ds.Select(myStruct2{}).ToSQL()
	fmt.Println(sql)

	var myStructs []myStruct

	// Pass slice of structs, will only select columns from underlying type
	sql, _, _ = ds.Select(myStructs).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT "address", "email_address", "name" FROM "test"
	// SELECT "address", "email_address", "name" FROM "test"
	// SELECT "address", "email_address", "name", "zipcode" FROM "test"
	// SELECT "address", "email_address", "name", "zipcode" FROM "test"
	// SELECT "address", "email_address", "name" FROM "test"
}

func ExampleSelectDataset_Distinct() {
	sql, _, _ := builder.From("test").Select("a", "b").Distinct().ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT DISTINCT "a", "b" FROM "test"
}

func ExampleSelectDataset_Distinct_on() {
	sql, _, _ := builder.From("test").Distinct("a").ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT DISTINCT ON ("a") * FROM "test"
}

func ExampleSelectDataset_Distinct_onWithLiteral() {
	sql, _, _ := builder.From("test").Distinct(builder.L("COALESCE(?, ?)", builder.C("a"), "empty")).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT DISTINCT ON (COALESCE("a", 'empty')) * FROM "test"
}

func ExampleSelectDataset_Distinct_onCoalesce() {
	sql, _, _ := builder.From("test").Distinct(builder.COALESCE(builder.C("a"), "empty")).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT DISTINCT ON (COALESCE("a", 'empty')) * FROM "test"
}

func ExampleSelectDataset_SelectAppend() {
	ds := builder.From("test").Select("a", "b")
	sql, _, _ := ds.SelectAppend("c").ToSQL()
	fmt.Println(sql)
	ds = builder.From("test").Select("a", "b").Distinct()
	sql, _, _ = ds.SelectAppend("c").ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT "a", "b", "c" FROM "test"
	// SELECT DISTINCT "a", "b", "c" FROM "test"
}

func ExampleSelectDataset_ClearSelect() {
	ds := builder.From("test").Select("a", "b")
	sql, _, _ := ds.ClearSelect().ToSQL()
	fmt.Println(sql)
	ds = builder.From("test").Select("a", "b").Distinct()
	sql, _, _ = ds.ClearSelect().ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
	// SELECT * FROM "test"
}

func ExampleSelectDataset_ToSQL() {
	sql, args, _ := builder.From("items").Where(builder.Ex{"a": 1}).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "items" WHERE ("a" = 1) []
}

func ExampleSelectDataset_ToSQL_prepared() {
	sql, args, _ := builder.From("items").Where(builder.Ex{"a": 1}).Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "items" WHERE ("a" = ?) [1]
}

func ExampleSelectDataset_Update() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := builder.From("items").Update().Set(
		item{Name: "Test", Address: "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("items").Update().Set(
		builder.Record{"name": "Test", "address": "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("items").Update().Set(
		map[string]any{"name": "Test", "address": "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
}

func ExampleSelectDataset_Insert() {
	type item struct {
		ID      uint32 `db:"id" builder:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := builder.From("items").Insert().Rows(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("items").Insert().Rows(
		builder.Record{"name": "Test1", "address": "111 Test Addr"},
		builder.Record{"name": "Test2", "address": "112 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("items").Insert().Rows(
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		}).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("items").Insert().Rows(
		[]builder.Record{
			{"name": "Test1", "address": "111 Test Addr"},
			{"name": "Test2", "address": "112 Test Addr"},
		}).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
}

func ExampleSelectDataset_Delete() {
	sql, args, _ := builder.From("items").Delete().ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = builder.From("items").
		Where(builder.Ex{"id": builder.Op{"gt": 10}}).
		Delete().
		ToSQL()
	fmt.Println(sql, args)

	// Output:
	// DELETE FROM "items" []
	// DELETE FROM "items" WHERE ("id" > 10) []
}

func ExampleSelectDataset_Truncate() {
	sql, args, _ := builder.From("items").Truncate().ToSQL()
	fmt.Println(sql, args)
	// Output:
	// TRUNCATE "items" []
}

func ExampleSelectDataset_Prepared() {
	sql, args, _ := builder.From("items").Prepared(true).Where(builder.Ex{
		"col1": "a",
		"col2": 1,
		"col3": true,
		"col4": false,
		"col5": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql, args)
	// nolint:lll // sql statements are long
	// Output:
	// SELECT * FROM "items" WHERE (("col1" = ?) AND ("col2" = ?) AND ("col3" IS TRUE) AND ("col4" IS FALSE) AND ("col5" IN (?, ?, ?))) [a 1 a b c]
}

func ExampleSelectDataset_QueryRows() {
	type User struct {
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
	}
	db := getDB()
	var users []User
	if err := db.From("builder_user").QueryRows(&users); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("\n%+v", users)

	users = users[0:0]
	if err := db.From("builder_user").Select("first_name").QueryRowsPartial(&users); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("\n%+v", users)

	// Output:
	// [{FirstName:Bob LastName:Yukon} {FirstName:Sally LastName:Yukon} {FirstName:Vinita LastName:Yukon} {FirstName:John LastName:Doe}]
	// [{FirstName:Bob LastName:} {FirstName:Sally LastName:} {FirstName:Vinita LastName:} {FirstName:John LastName:}]
}

func ExampleSelectDataset_QueryRows_prepared() {
	type User struct {
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
	}
	db := getDB()

	ds := db.From("builder_user").
		Prepared(true).
		Where(builder.Ex{
			"last_name": "Yukon",
		})

	var users []User
	if err := ds.QueryRows(&users); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("\n%+v", users)

	// Output:
	// [{FirstName:Bob LastName:Yukon} {FirstName:Sally LastName:Yukon} {FirstName:Vinita LastName:Yukon}]
}

// In this example we create a new struct that has two structs that represent two table
// the User and Role fields are tagged with the table name
func ExampleSelectDataset_QueryRows_withJoinAutoSelect() {
	type Role struct {
		UserID uint64 `db:"user_id"`
		Name   string `db:"name"`
	}
	type User struct {
		ID        uint64 `db:"id"`
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
	}
	type UserAndRole struct {
		User // tag as the "builder_user" table
		Role // tag as "user_role" table
	}
	db := getDB()

	ds := db.
		From("builder_user").
		Join(builder.T("user_role"), builder.On(builder.I("builder_user.id").Eq(builder.I("user_role.user_id"))))
	var users []UserAndRole
	// query rows will auto build the
	err := ds.QueryRows(&users)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	for _, u := range users {
		fmt.Printf("\n%+v", u)
	}
	// Output:
	// {User:{ID:1 FirstName:Bob LastName:Yukon} Role:{UserID:1 Name:Admin}}
	// {User:{ID:2 FirstName:Sally LastName:Yukon} Role:{UserID:2 Name:Manager}}
	// {User:{ID:3 FirstName:Vinita LastName:Yukon} Role:{UserID:3 Name:Manager}}
	// {User:{ID:4 FirstName:John LastName:Doe} Role:{UserID:4 Name:User}}
}

func ExampleSelectDataset_QueryRow() {
	type User struct {
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
	}
	db := getDB()
	findUserByName := func(name string) {
		var user User
		ds := db.From("builder_user").Where(builder.C("first_name").Eq(name))
		err := ds.QueryRowPartial(&user)
		switch {
		case err != nil:
			fmt.Println(err.Error())
		default:
			fmt.Printf("Found user: %+v\n", user)
		}
	}

	findUserByName("Bob")
	findUserByName("Zeb")

	// Output:
	// Found user: {FirstName:Bob LastName:Yukon}
	// sql: no rows in result set
}

// In this example we create a new struct that has two structs that represent two table
// the User and Role fields are tagged with the table name
func ExampleSelectDataset_QueryRow_withJoinAutoSelect() {
	type Role struct {
		UserID uint64 `db:"user_id"`
		Name   string `db:"name"`
	}
	type User struct {
		ID        uint64 `db:"id"`
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
	}
	type UserAndRole struct {
		User // tag as the "builder_user" table
		Role // tag as "user_role" table
	}
	db := getDB()
	findUserAndRoleByName := func(name string) {
		var userAndRole UserAndRole
		ds := db.
			From("builder_user").
			Join(
				builder.T("user_role"),
				builder.On(builder.I("builder_user.id").Eq(builder.I("user_role.user_id"))),
			).
			Where(builder.C("first_name").Eq(name))
		err := ds.QueryRow(&userAndRole)
		switch {
		case err != nil:
			fmt.Println(err.Error())
		default:
			fmt.Printf("Found user and role: %+v\n", userAndRole)
		}
	}

	findUserAndRoleByName("Bob")
	findUserAndRoleByName("Zeb")
	// Output:
	// Found user and role: {User:{ID:1 FirstName:Bob LastName:Yukon} Role:{UserID:1 Name:Admin}}
	// sql: no rows in result set
}

func ExampleSelectDataset_QueryRowsPartial() {
	var ids []int64
	if err := getDB().From("builder_user").Select("id").QueryRows(&ids); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("UserIds = %+v", ids)

	// Output:
	// UserIds = [1 2 3 4]
}

func ExampleSelectDataset_QueryRowPartial() {
	db := getDB()
	findUserIDByName := func(name string) {
		var id int64
		ds := db.From("builder_user").
			Select("id").
			Where(builder.C("first_name").Eq(name))

		err := ds.QueryRow(&id)
		switch {
		case err != nil:
			fmt.Println(err.Error())
		default:
			fmt.Printf("\nFound userId: %+v\n", id)
		}
	}

	findUserIDByName("Bob")
	findUserIDByName("Zeb")
	// Output:
	// Found userId: 1
	// sql: no rows in result set
}

func ExampleSelectDataset_Count() {
	count, err := getDB().From("builder_user").Count()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("Count is %d", count)

	// Output:
	// Count is 4
}

func ExampleSelectDataset_Pluck() {
	var lastNames []string
	if err := getDB().From("builder_user").Pluck(&lastNames, "last_name"); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("LastNames = %+v", lastNames)

	// Output:
	// LastNames = [Yukon Yukon Yukon Doe]
}

func ExampleSelectDataset_Executor_scannerQueryRow() {
	type User struct {
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
	}
	db := getDB()

	insertDataset := db.
		From("builder_user").
		Select("first_name", "last_name").
		Where(builder.Ex{
			"last_name": "Yukon",
		})

	users := []User{}
	err := insertDataset.QueryRows(&users)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	for _, user := range users {
		fmt.Printf("\n%+v", user)
	}

	// Output:
	// {FirstName:Bob LastName:Yukon}
	// {FirstName:Sally LastName:Yukon}
	// {FirstName:Vinita LastName:Yukon}
}

func ExampleSelectDataset_Executor_scannerQueryRowPartial() {
	db := getDB()

	dataset := db.
		From("builder_user").
		Select("first_name").
		Where(builder.Ex{
			"last_name": "Yukon",
		})

	names := []string{}
	err := dataset.QueryRows(&names)
	if err != nil {
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	}
	for _, name := range names {
		fmt.Println(name)
	}

	// Output:
	// Bob
	// Sally
	// Vinita
}

func ExampleForUpdate() {
	sql, args, _ := builder.From("test").ForUpdate(exp.Wait).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" FOR UPDATE  []
}

func ExampleForUpdate_of() {
	sql, args, _ := builder.From("test").ForUpdate(exp.Wait, builder.T("test")).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" FOR UPDATE OF "test"  []
}

func ExampleForUpdate_ofMultiple() {
	sql, args, _ := builder.From("table1").Join(
		builder.T("table2"),
		builder.On(builder.I("table2.id").Eq(builder.I("table1.id"))),
	).ForUpdate(
		exp.Wait,
		builder.T("table1"),
		builder.T("table2"),
	).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "table1" INNER JOIN "table2" ON ("table2"."id" = "table1"."id") FOR UPDATE OF "table1", "table2"  []
}
