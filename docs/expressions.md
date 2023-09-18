# Expressions

`builder` provides an idiomatic DSL for generating SQL. Datasets only act as a clause builder (i.e. Where, From, Select), most of these clause methods accept Expressions which are the building blocks for your SQL statement, you can think of them as fragments of SQL.

* [`Ex{}`](#ex) - A map where the key will become an Identifier and the Key is the value, this is most commonly used in the Where clause.
* [`ExOr{}`](#ex-or)- OR version of `Ex`. A map where the key will become an Identifier and the Key is the value, this is most commonly used in the Where clause
* [`S`](#S) - An Identifier that represents a schema. With a schema identifier you can fully qualify tables and columns.
* [`T`](#T) - An Identifier that represents a Table. With a Table identifier you can fully qualify columns.
* [`C`](#C) - An Identifier that represents a Column. See the docs for more examples
* [`I`](#I) - An Identifier represents a schema, table, or column or any combination. I parses identifiers seperated by a . character.
* [`L`](#L) - An SQL literal.
* [`V`](#V) - An Value to be used in SQL. 
* [`And`](#and) - AND multiple expressions together.
* [`Or`](#or) - OR multiple expressions together.
* [Complex Example](#complex) - Complex Example using most of the Expression DSL.

The entry points for expressions are:

<a name="ex"></a>
**[`Ex{}`](https://godoc.org/github.com/Tooooommy/builder#Ex)** 

A map where the key will become an Identifier and the Key is the value, this is most commonly used in the Where clause. By default `Ex` will use the equality operator except in cases where the equality operator will not work, see the example below.

```go
sql, _, _ := db.From("items").Where(builder.Ex{
  "col1": "a",
  "col2": 1,
  "col3": true,
  "col4": false,
  "col5": nil,
  "col6": []string{"a", "b", "c"},
}).ToSQL()
fmt.Println(sql)
```

Output:
```sql
SELECT * FROM "items" WHERE (("col1" = 'a') AND ("col2" = 1) AND ("col3" IS TRUE) AND ("col4" IS FALSE) AND ("col5" IS NULL) AND ("col6" IN ('a', 'b', 'c')))
```

You can also use the [`Op`](https://godoc.org/github.com/Tooooommy/builder#Op) map which allows you to create more complex expressions using the map syntax. When using the `Op` map the key is the name of the comparison you want to make (e.g. `"neq"`, `"like"`, `"is"`, `"in"`), the key is case insensitive.

```go
sql, _, _ := db.From("items").Where(builder.Ex{
  "col1": builder.Op{"neq": "a"},
  "col3": builder.Op{"isNot": true},
  "col6": builder.Op{"notIn": []string{"a", "b", "c"}},
}).ToSQL()
fmt.Println(sql)
```

Output:
```sql
SELECT * FROM "items" WHERE (("col1" != 'a') AND ("col3" IS NOT TRUE) AND ("col6" NOT IN ('a', 'b', 'c')))
```
For a more complete examples see the [`Op`](https://godoc.org/github.com/Tooooommy/builder#Op) and [`Ex`](https://godoc.org/github.com/Tooooommy/builder#Ex) docs

<a name="ex-or"></a>
**[`ExOr{}`](https://godoc.org/github.com/Tooooommy/builder#ExOr)** 

A map where the key will become an Identifier and the Key is the value, this is most commonly used in the Where clause. By default `ExOr` will use the equality operator except in cases where the equality operator will not work, see the example below.

```go
sql, _, _ := db.From("items").Where(builder.ExOr{
  "col1": "a",
  "col2": 1,
  "col3": true,
  "col4": false,
  "col5": nil,
  "col6": []string{"a", "b", "c"},
}).ToSQL()
fmt.Println(sql)
```
  
Output:
```sql
SELECT * FROM "items" WHERE (("col1" = 'a') OR ("col2" = 1) OR ("col3" IS TRUE) OR ("col4" IS FALSE) OR ("col5" IS NULL) OR ("col6" IN ('a', 'b', 'c')))
```
  
You can also use the [`Op`](https://godoc.org/github.com/Tooooommy/builder#Op) map which allows you to create more complex expressions using the map syntax. When using the `Op` map the key is the name of the comparison you want to make (e.g. `"neq"`, `"like"`, `"is"`, `"in"`), the key is case insensitive.
  
```go
sql, _, _ := db.From("items").Where(builder.ExOr{
  "col1": builder.Op{"neq": "a"},
  "col3": builder.Op{"isNot": true},
  "col6": builder.Op{"notIn": []string{"a", "b", "c"}},
}).ToSQL()
fmt.Println(sql)
```
  
Output:
```sql
SELECT * FROM "items" WHERE (("col1" != 'a') OR ("col3" IS NOT TRUE) OR ("col6" NOT IN ('a', 'b', 'c')))
```
For a more complete examples see the [`Op`](https://godoc.org/github.com/Tooooommy/builder#Op) and [`ExOr`](https://godoc.org/github.com/Tooooommy/builder#Ex) docs

<a name="S"></a>
**[`S()`](https://godoc.org/github.com/Tooooommy/builder#S)**

An Identifier that represents a schema. With a schema identifier you can fully qualify tables and columns.

```go
s := builder.S("my_schema")

// "my_schema"."my_table"
t := s.Table("my_table")

// "my_schema"."my_table"."my_column"

sql, _, _ := builder.From(t).Select(t.Col("my_column").ToSQL()
// SELECT "my_schema"."my_table"."my_column" FROM "my_schema"."my_table"
fmt.Println(sql)
```

<a name="T"></a>
**[`T()`](https://godoc.org/github.com/Tooooommy/builder#T)** 

An Identifier that represents a Table. With a Table identifier you can fully qualify columns.
```go
t := s.Table("my_table")

sql, _, _ := builder.From(t).Select(t.Col("my_column").ToSQL()
// SELECT "my_table"."my_column" FROM "my_table"
fmt.Println(sql)

// qualify the table with a schema
sql, _, _ := builder.From(t.Schema("my_schema")).Select(t.Col("my_column").ToSQL()
// SELECT "my_table"."my_column" FROM "my_schema"."my_table"
fmt.Println(sql)
```

<a name="C"></a>
**[`C()`](https://godoc.org/github.com/Tooooommy/builder#C)** 

An Identifier that represents a Column. See the [docs]((https://godoc.org/github.com/Tooooommy/builder#C)) for more examples

```go
sql, _, _ := builder.From("table").Where(builder.C("col").Eq(10)).ToSQL()
// SELECT * FROM "table" WHERE "col" = 10
fmt.Println(sql)
```

<a name="I"></a>
**[`I()`](https://godoc.org/github.com/Tooooommy/builder#I)** 

An Identifier represents a schema, table, or column or any combination. `I` parses identifiers seperated by a `.` character.

```go
// with three parts it is assumed you have provided a schema, table and column
builder.I("my_schema.table.col") == builder.S("my_schema").Table("table").Col("col")

// with two parts it is assumed you have provided a table and column
builder.I("table.col") == builder.T("table").Col("col")

// with a single value it is the same as calling builder.C
builder.I("col") == builder.C("col")
```

<a name="L"></a>
**[`L()`](https://godoc.org/github.com/Tooooommy/builder#L)** 

An SQL literal. You may find yourself in a situation where an IdentifierExpression cannot expression an SQL fragment that your database supports. In that case you can use a LiteralExpression

```go
// manual casting
builder.L(`"json"::TEXT = "other_json"::text`)

// custom function invocation
builder.L(`custom_func("a")`)

// postgres JSON access
builder.L(`"json_col"->>'someField'`).As("some_field")
```
  
You can also use placeholders in your literal with a `?` character. `builder` will handle changing it to what the dialect needs (e.g. `?` mysql, `$1` postgres, `?` sqlite3). 

**NOTE** If your query is not prepared the placeholders will be properly interpolated.

```go
builder.L("col IN (?, ?, ?)", "a", "b", "c") 
```

Putting it together
  
```go
ds := db.From("test").Where(
  builder.L(`("json"::TEXT = "other_json"::TEXT)`),
  builder.L("col IN (?, ?, ?)", "a", "b", "c"),
)

sql, args, _ := ds.ToSQL()
fmt.Println(sql, args)

sql, args, _ := ds.Prepared(true).ToSQL()
fmt.Println(sql, args)
```

Output:
```sql
SELECT * FROM "test" WHERE ("json"::TEXT = "other_json"::TEXT) AND col IN ('a', 'b', 'c') []
-- assuming postgres dialect
SELECT * FROM "test" WHERE ("json"::TEXT = "other_json"::TEXT) AND col IN ($1, $2, $3) [a, b, c]
```

<a name="V"></a>
**[`V()`](https://godoc.org/github.com/Tooooommy/builder#V)**

Sometimes you may have a value that you want to use directly in SQL. 

**NOTE** This is a shorter version of `builder.L("?", val)`

For example you may want to select a value as a column.

```go
ds := builder.From("user").Select(
	builder.V(true).As("is_verified"),
	builder.V(1.2).As("version"),
	"first_name",
	"last_name",
)

sql, args, _ := ds.ToSQL()
fmt.Println(sql, args)
```

Output:
```
SELECT TRUE AS "is_verified", 1.2 AS "version", "first_name", "last_name" FROM "user" []
```

You can also use `builder.V` in where clauses.

```
ds := builder.From("user").Where(builder.V(1).Neq(1))
sql, args, _ := ds.ToSQL()
fmt.Println(sql, args)
```

Output:

```
SELECT * FROM "user" WHERE (1 != 1) []
```

You can also use them in prepared statements.

```
ds := builder.From("user").Where(builder.V(1).Neq(1))
sql, args, _ := ds.Prepared(true).ToSQL()
fmt.Println(sql, args)
```

Output:

```
SELECT * FROM "user" WHERE (? != ?) [1, 1]
```


<a name="and"></a>
**[`And()`](https://godoc.org/github.com/Tooooommy/builder#And)** 

You can use the `And` function to AND multiple expressions together.

**NOTE** By default builder will AND expressions together

```go
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
```

Output:
```sql
SELECT * FROM "test" WHERE (("col" > 10) AND ("col" < 20)) []
SELECT * FROM "test" WHERE (("col" > ?) AND ("col" < ?)) [10 20]
```

<a name="or"></a>
**[`Or()`](https://godoc.org/github.com/Tooooommy/builder#Or)** 

You can use the `Or` function to OR multiple expressions together.

```go
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
```

Output:
```sql
SELECT * FROM "test" WHERE (("col" = 10) OR ("col" = 20)) []
SELECT * FROM "test" WHERE (("col" = ?) OR ("col" = ?)) [10 20]
```

You can also use `Or` and `And` functions in tandem which will give you control not only over how the Expressions are joined together, but also how they are grouped
 
```go
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
```

Output:
```sql
SELECT * FROM "items" WHERE (("a" > 10) OR (("b" = 100) AND ("c" != 'test'))) []
SELECT * FROM "items" WHERE (("a" > ?) OR (("b" = ?) AND ("c" != ?))) [10 100 test]
```

You can also use Or with the map syntax
```go
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
```

Output:
```sql
SELECT * FROM "test" WHERE ((("col1" = 1) AND ("col2" IS TRUE)) OR (("col3" IS NULL) AND ("col4" = 'foo'))) []
SELECT * FROM "test" WHERE ((("col1" = ?) AND ("col2" IS TRUE)) OR (("col3" IS NULL) AND ("col4" = ?))) [1 foo]
```

<a name="complex"></a>
## Complex Example

This example uses most of the features of the `builder` Expression DSL

```go
ds := db.From("test").
  Select(builder.COUNT("*")).
  InnerJoin(builder.I("test2"), builder.On(builder.Ex{"test.fkey": builder.I("test2.id")})).
  LeftJoin(builder.I("test3"), builder.On(builder.Ex{"test2.fkey": builder.I("test3.id")})).
  Where(
    builder.Ex{
      "test.name":    builder.Op{"like": regexp.MustCompile("^(a|b)")},
      "test2.amount": builder.Op{"isNot": nil},
    },
    builder.ExOr{
      "test3.id":     nil,
      "test3.status": []string{"passed", "active", "registered"},
    },
  ).
  Order(builder.I("test.created").Desc().NullsLast()).
  GroupBy(builder.I("test.user_id")).
  Having(builder.AVG("test3.age").Gt(10))

sql, args, _ := ds.ToSQL()
fmt.Println(sql)

sql, args, _ := ds.Prepared(true).ToSQL()
fmt.Println(sql)
```

Using the Expression syntax
```go
ds := db.From("test").
  Select(builder.COUNT("*")).
  InnerJoin(builder.I("test2"), builder.On(builder.I("test.fkey").Eq(builder.I("test2.id")))).
  LeftJoin(builder.I("test3"), builder.On(builder.I("test2.fkey").Eq(builder.I("test3.id")))).
  Where(
    builder.I("test.name").Like(regexp.MustCompile("^(a|b)")),
    builder.I("test2.amount").IsNotNull(),
    builder.Or(
      builder.I("test3.id").IsNull(),
      builder.I("test3.status").In("passed", "active", "registered"),
    ),
  ).
  Order(builder.I("test.created").Desc().NullsLast()).
  GroupBy(builder.I("test.user_id")).
  Having(builder.AVG("test3.age").Gt(10))

sql, args, _ := ds.ToSQL()
fmt.Println(sql)

sql, args, _ := ds.Prepared(true).ToSQL()
fmt.Println(sql)
```

Both examples generate the following SQL

```sql
-- interpolated
SELECT COUNT(*)
FROM "test"
         INNER JOIN "test2" ON ("test"."fkey" = "test2"."id")
         LEFT JOIN "test3" ON ("test2"."fkey" = "test3"."id")
WHERE ((("test"."name" ~ '^(a|b)') AND ("test2"."amount" IS NOT NULL)) AND
       (("test3"."id" IS NULL) OR ("test3"."status" IN ('passed', 'active', 'registered'))))
GROUP BY "test"."user_id"
HAVING (AVG("test3"."age") > 10)
ORDER BY "test"."created" DESC NULLS LAST []

-- prepared
SELECT COUNT(*)
FROM "test"
         INNER JOIN "test2" ON ("test"."fkey" = "test2"."id")
         LEFT JOIN "test3" ON ("test2"."fkey" = "test3"."id")
WHERE ((("test"."name" ~ ?) AND ("test2"."amount" IS NOT NULL)) AND
       (("test3"."id" IS NULL) OR ("test3"."status" IN (?, ?, ?))))
GROUP BY "test"."user_id"
HAVING (AVG("test3"."age") > ?)
ORDER BY "test"."created" DESC NULLS LAST [^(a|b) passed active registered 10]
```


