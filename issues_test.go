package builder_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Tooooommy/builder/v9"
	"github.com/Tooooommy/builder/v9/exp"
	"github.com/stretchr/testify/suite"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type githubIssuesSuite struct {
	suite.Suite
}

func (gis *githubIssuesSuite) AfterTest(suiteName, testName string) {
	builder.SetColumnRenameFunction(strings.ToLower)
}

// Test for https://github.com/Tooooommy/builder/issues/49
func (gis *githubIssuesSuite) TestIssue49() {
	dialect := builder.Dialect("default")

	filters := builder.Or()
	sql, args, err := dialect.From("table").Where(filters).ToSQL()
	gis.NoError(err)
	gis.Empty(args)
	gis.Equal(`SELECT * FROM "table"`, sql)

	sql, args, err = dialect.From("table").Where(builder.Ex{}).ToSQL()
	gis.NoError(err)
	gis.Empty(args)
	gis.Equal(`SELECT * FROM "table"`, sql)

	sql, args, err = dialect.From("table").Where(builder.ExOr{}).ToSQL()
	gis.NoError(err)
	gis.Empty(args)
	gis.Equal(`SELECT * FROM "table"`, sql)
}

// Test for https://github.com/Tooooommy/builder/issues/115
func (gis *githubIssuesSuite) TestIssue115() {
	type TestStruct struct {
		Field string
	}
	builder.SetColumnRenameFunction(func(col string) string {
		return ""
	})

	_, _, err := builder.Insert("test").Rows(TestStruct{Field: "hello"}).ToSQL()
	gis.EqualError(err, `builder: a empty identifier was encountered, please specify a "schema", "table" or "column"`)
}

// Test for https://github.com/Tooooommy/builder/issues/118
func (gis *githubIssuesSuite) TestIssue118_withEmbeddedStructWithoutExportedFields() {
	// struct is in a custom package
	type SimpleRole struct {
		sync.RWMutex
		permissions []string // nolint:structcheck,unused //needed for test
	}

	// .....

	type Role struct {
		*SimpleRole

		ID        string    `json:"id" db:"id" builder:"skipinsert"`
		Key       string    `json:"key" db:"key"`
		Name      string    `json:"name" db:"name"`
		CreatedAt time.Time `json:"-" db:"created_at" builder:"skipinsert"`
	}

	rUser := &Role{
		Key:  `user`,
		Name: `User role`,
	}

	sql, arg, err := builder.Insert(`rbac_roles`).
		Returning(builder.C(`id`)).
		Rows(rUser).
		ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(`INSERT INTO "rbac_roles" ("key", "name") VALUES ('user', 'User role') RETURNING "id"`, sql)

	sql, arg, err = builder.Update(`rbac_roles`).
		Returning(builder.C(`id`)).
		Set(rUser).
		ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(
		`UPDATE "rbac_roles" SET "created_at"='0001-01-01T00:00:00Z',"id"='',"key"='user',"name"='User role' RETURNING "id"`,
		sql,
	)

	rUser = &Role{
		SimpleRole: &SimpleRole{},
		Key:        `user`,
		Name:       `User role`,
	}

	sql, arg, err = builder.Insert(`rbac_roles`).
		Returning(builder.C(`id`)).
		Rows(rUser).
		ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(`INSERT INTO "rbac_roles" ("key", "name") VALUES ('user', 'User role') RETURNING "id"`, sql)

	sql, arg, err = builder.Update(`rbac_roles`).
		Returning(builder.C(`id`)).
		Set(rUser).
		ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(
		`UPDATE "rbac_roles" SET `+
			`"created_at"='0001-01-01T00:00:00Z',"id"='',"key"='user',"name"='User role' RETURNING "id"`,
		sql,
	)
}

// Test for https://github.com/Tooooommy/builder/issues/118
func (gis *githubIssuesSuite) TestIssue118_withNilEmbeddedStructWithExportedFields() {
	// struct is in a custom package
	type SimpleRole struct {
		sync.RWMutex
		permissions []string // nolint:structcheck,unused // needed for test
		IDStr       string
	}

	// .....

	type Role struct {
		*SimpleRole

		ID        string    `json:"id" db:"id" builder:"skipinsert"`
		Key       string    `json:"key" db:"key"`
		Name      string    `json:"name" db:"name"`
		CreatedAt time.Time `json:"-" db:"created_at" builder:"skipinsert"`
	}

	rUser := &Role{
		Key:  `user`,
		Name: `User role`,
	}
	sql, arg, err := builder.Insert(`rbac_roles`).
		Returning(builder.C(`id`)).
		Rows(rUser).
		ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	// it should not insert fields on nil embedded pointers
	gis.Equal(`INSERT INTO "rbac_roles" ("key", "name") VALUES ('user', 'User role') RETURNING "id"`, sql)

	sql, arg, err = builder.Update(`rbac_roles`).
		Returning(builder.C(`id`)).
		Set(rUser).
		ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	// it should not insert fields on nil embedded pointers
	gis.Equal(
		`UPDATE "rbac_roles" SET "created_at"='0001-01-01T00:00:00Z',"id"='',"key"='user',"name"='User role' RETURNING "id"`,
		sql,
	)

	rUser = &Role{
		SimpleRole: &SimpleRole{},
		Key:        `user`,
		Name:       `User role`,
	}
	sql, arg, err = builder.Insert(`rbac_roles`).
		Returning(builder.C(`id`)).
		Rows(rUser).
		ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	// it should not insert fields on nil embedded pointers
	gis.Equal(
		`INSERT INTO "rbac_roles" ("idstr", "key", "name") VALUES ('', 'user', 'User role') RETURNING "id"`,
		sql,
	)

	sql, arg, err = builder.Update(`rbac_roles`).
		Returning(builder.C(`id`)).
		Set(rUser).
		ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	// it should not insert fields on nil embedded pointers
	gis.Equal(
		`UPDATE "rbac_roles" SET `+
			`"created_at"='0001-01-01T00:00:00Z',"id"='',"idstr"='',"key"='user',"name"='User role' RETURNING "id"`,
		sql,
	)
}

// Test for https://github.com/Tooooommy/builder/issues/118
func (gis *githubIssuesSuite) TestIssue140() {
	sql, arg, err := builder.Insert(`test`).Returning().ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(`INSERT INTO "test" DEFAULT VALUES`, sql)

	sql, arg, err = builder.Update(`test`).Set(builder.Record{"a": "b"}).Returning().ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(
		`UPDATE "test" SET "a"='b'`,
		sql,
	)

	sql, arg, err = builder.Delete(`test`).Returning().ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(
		`DELETE FROM "test"`,
		sql,
	)

	sql, arg, err = builder.Insert(`test`).Returning(nil).ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(`INSERT INTO "test" DEFAULT VALUES`, sql)

	sql, arg, err = builder.Update(`test`).Set(builder.Record{"a": "b"}).Returning(nil).ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(
		`UPDATE "test" SET "a"='b'`,
		sql,
	)

	sql, arg, err = builder.Delete(`test`).Returning(nil).ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(
		`DELETE FROM "test"`,
		sql,
	)
}

// Test for https://github.com/Tooooommy/builder/issues/164
func (gis *githubIssuesSuite) TestIssue164() {
	insertDs := builder.Insert("foo").Rows(builder.Record{"user_id": 10}).Returning("id")

	ds := builder.From("bar").
		With("ins", insertDs).
		Select("bar_name").
		Where(builder.Ex{"bar.user_id": builder.I("ins.user_id")})

	sql, args, err := ds.ToSQL()
	gis.NoError(err)
	gis.Empty(args)
	gis.Equal(
		`WITH ins AS (INSERT INTO "foo" ("user_id") VALUES (10) RETURNING "id") `+
			`SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "ins"."user_id")`,
		sql,
	)

	sql, args, err = ds.Prepared(true).ToSQL()
	gis.NoError(err)
	gis.Equal([]any{int64(10)}, args)
	gis.Equal(
		`WITH ins AS (INSERT INTO "foo" ("user_id") VALUES (?) RETURNING "id")`+
			` SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "ins"."user_id")`,
		sql,
	)

	updateDs := builder.Update("foo").Set(builder.Record{"bar": "baz"}).Returning("id")

	ds = builder.From("bar").
		With("upd", updateDs).
		Select("bar_name").
		Where(builder.Ex{"bar.user_id": builder.I("upd.user_id")})

	sql, args, err = ds.ToSQL()
	gis.NoError(err)
	gis.Empty(args)
	gis.Equal(
		`WITH upd AS (UPDATE "foo" SET "bar"='baz' RETURNING "id") SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "upd"."user_id")`,
		sql,
	)

	sql, args, err = ds.Prepared(true).ToSQL()
	gis.NoError(err)
	gis.Equal([]any{"baz"}, args)
	gis.Equal(
		`WITH upd AS (UPDATE "foo" SET "bar"=? RETURNING "id") SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "upd"."user_id")`,
		sql,
	)

	deleteDs := builder.Delete("foo").Where(builder.Ex{"bar": "baz"}).Returning("id")

	ds = builder.From("bar").
		With("del", deleteDs).
		Select("bar_name").
		Where(builder.Ex{"bar.user_id": builder.I("del.user_id")})

	sql, args, err = ds.ToSQL()
	gis.NoError(err)
	gis.Empty(args)
	gis.Equal(
		`WITH del AS (DELETE FROM "foo" WHERE ("bar" = 'baz') RETURNING "id")`+
			` SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "del"."user_id")`,
		sql,
	)

	sql, args, err = ds.Prepared(true).ToSQL()
	gis.NoError(err)
	gis.Equal([]any{"baz"}, args)
	gis.Equal(
		`WITH del AS (DELETE FROM "foo" WHERE ("bar" = ?) RETURNING "id")`+
			` SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "del"."user_id")`,
		sql,
	)
}

// Test for https://github.com/Tooooommy/builder/issues/177
func (gis *githubIssuesSuite) TestIssue177() {
	ds := builder.Dialect("postgres").
		From("ins1").
		With("ins1",
			builder.Dialect("postgres").
				Insert("account").
				Rows(builder.Record{"email": "email@email.com", "status": "active", "uuid": "XXX-XXX-XXXX"}).
				Returning("*"),
		).
		With("ins2",
			builder.Dialect("postgres").
				Insert("account_user").
				Cols("account_id", "user_id").
				FromQuery(builder.Dialect("postgres").
					From("ins1").
					Select(
						"id",
						builder.V(1001),
					),
				),
		).
		Select("*")
	sql, args, err := ds.ToSQL()
	gis.NoError(err)
	gis.Equal(`WITH ins1 AS (`+
		`INSERT INTO "account" ("email", "status", "uuid") VALUES ('email@email.com', 'active', 'XXX-XXX-XXXX') RETURNING *),`+
		` ins2 AS (INSERT INTO "account_user" ("account_id", "user_id") SELECT "id", 1001 FROM "ins1")`+
		` SELECT * FROM "ins1"`, sql)
	gis.Len(args, 0)

	sql, args, err = ds.Prepared(true).ToSQL()
	gis.NoError(err)
	gis.Equal(`WITH ins1 AS (INSERT INTO "account" ("email", "status", "uuid") VALUES ($1, $2, $3) RETURNING *), ins2`+
		` AS (INSERT INTO "account_user" ("account_id", "user_id") SELECT "id", $4 FROM "ins1") SELECT * FROM "ins1"`, sql)
	gis.Equal(args, []any{"email@email.com", "active", "XXX-XXX-XXXX", int64(1001)})
}

// Test for https://github.com/Tooooommy/builder/issues/183
func (gis *githubIssuesSuite) TestIssue184() {
	expectedErr := fmt.Errorf("an error")
	testCases := []struct {
		ds exp.AppendableExpression
	}{
		{ds: builder.From("test").As("t").SetError(expectedErr)},
		{ds: builder.Insert("test").Rows(builder.Record{"foo": "bar"}).Returning("foo").SetError(expectedErr)},
		{ds: builder.Update("test").Set(builder.Record{"foo": "bar"}).Returning("foo").SetError(expectedErr)},
		{ds: builder.Update("test").Set(builder.Record{"foo": "bar"}).Returning("foo").SetError(expectedErr)},
		{ds: builder.Delete("test").Returning("foo").SetError(expectedErr)},
	}

	for _, tc := range testCases {
		ds := builder.From(tc.ds)
		sql, args, err := ds.ToSQL()
		gis.Equal(expectedErr, err)
		gis.Empty(sql)
		gis.Empty(args)

		sql, args, err = ds.Prepared(true).ToSQL()
		gis.Equal(expectedErr, err)
		gis.Empty(sql)
		gis.Empty(args)

		ds = builder.From("test2").Where(builder.Ex{"foo": tc.ds})

		sql, args, err = ds.ToSQL()
		gis.Equal(expectedErr, err)
		gis.Empty(sql)
		gis.Empty(args)

		sql, args, err = ds.Prepared(true).ToSQL()
		gis.Equal(expectedErr, err)
		gis.Empty(sql)
		gis.Empty(args)
	}
}

// Test for https://github.com/Tooooommy/builder/issues/185
func (gis *githubIssuesSuite) TestIssue185() {
	mDB, sqlMock, err := sqlmock.New()
	gis.NoError(err)
	sqlMock.ExpectQuery(
		`SELECT "id" FROM \(SELECT "id" FROM "table" ORDER BY "id" ASC\) AS "t1" UNION 
\(SELECT \* FROM \(SELECT "id" FROM "table" ORDER BY "id" ASC\) AS "t1"\)`,
	).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n"))

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)

	ds := db.Select("id").From("table").Order(builder.C("id").Asc()).
		Union(
			db.Select("id").From("table").Order(builder.C("id").Asc()),
		)

	ctx := context.Background()
	var ids []int
	gis.NoError(ds.Select("id").QueryRowsPartialCtx(ctx, &ids))
	gis.Equal([]int{1, 2, 3, 4}, ids)
}

// Test for https://github.com/Tooooommy/builder/issues/203
func (gis *githubIssuesSuite) TestIssue203() {
	// Schema definitions.
	authSchema := builder.S("company_auth")

	// Table definitions
	usersTable := authSchema.Table("users")

	u := usersTable.As("u")

	ds := builder.From(u).Select(
		u.Col("id"),
		u.Col("name"),
		u.Col("created_at"),
		u.Col("updated_at"),
	)

	sql, args, err := ds.ToSQL()
	gis.NoError(err)
	gis.Equal(`SELECT "u"."id", "u"."name", "u"."created_at", "u"."updated_at" FROM "company_auth"."users" AS "u"`, sql)
	gis.Empty(args, []any{})

	sql, args, err = ds.Prepared(true).ToSQL()
	gis.NoError(err)
	gis.Equal(`SELECT "u"."id", "u"."name", "u"."created_at", "u"."updated_at" FROM "company_auth"."users" AS "u"`, sql)
	gis.Empty(args, []any{})
}

func (gis *githubIssuesSuite) TestIssue290() {
	type OcomModel struct {
		ID           uint      `json:"id" db:"id" builder:"skipinsert"`
		CreatedDate  time.Time `json:"created_date" db:"created_date" builder:"skipupdate"`
		ModifiedDate time.Time `json:"modified_date" db:"modified_date"`
	}

	type ActiveModel struct {
		OcomModel
		ActiveStartDate time.Time  `json:"active_start_date" db:"active_start_date"`
		ActiveEndDate   *time.Time `json:"active_end_date" db:"active_end_date"`
	}

	type CodeModel struct {
		ActiveModel

		Code        string `json:"code" db:"code"`
		Description string `json:"description" binding:"required" db:"description"`
	}

	type CodeExample struct {
		CodeModel
	}

	var item CodeExample
	item.Code = "Code"
	item.Description = "Description"
	item.ID = 1 // Value set HERE!
	item.CreatedDate = time.Date(
		2021, 1, 1, 1, 1, 1, 1, time.UTC)
	item.ModifiedDate = time.Date(
		2021, 2, 2, 2, 2, 2, 2, time.UTC) // The Value we Get!
	item.ActiveStartDate = time.Date(
		2021, 3, 3, 3, 3, 3, 3, time.UTC)

	updateQuery := builder.From("example").Update().Set(item).Where(builder.C("id").Eq(1))

	sql, params, err := updateQuery.ToSQL()

	gis.NoError(err)
	gis.Empty(params)
	gis.Equal(`UPDATE "example" SET "active_end_date"=NULL,"active_start_date"='2021-03-03T03:03:03.000000003Z',"code"='Code',"description"='Description',"id"=1,"modified_date"='2021-02-02T02:02:02.000000002Z' WHERE ("id" = 1)`, sql) //nolint:lll
}

func TestGithubIssuesSuite(t *testing.T) {
	suite.Run(t, new(githubIssuesSuite))
}
