package builder_test

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Tooooommy/builder/v9"
	"github.com/Tooooommy/builder/v9/exp"
	"github.com/Tooooommy/builder/v9/internal/errors"
	"github.com/Tooooommy/builder/v9/internal/sb"
	"github.com/Tooooommy/builder/v9/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type (
	insertTestCase struct {
		ds      *builder.InsertDataset
		clauses exp.InsertClauses
	}
	insertDatasetSuite struct {
		suite.Suite
	}
)

func (ids *insertDatasetSuite) assertCases(cases ...insertTestCase) {
	for _, s := range cases {
		ids.Equal(s.clauses, s.ds.GetClauses())
	}
}

func (ids *insertDatasetSuite) TestInsert() {
	ds := builder.Insert("test")
	ids.IsType(&builder.InsertDataset{}, ds)
	ids.Implements((*exp.Expression)(nil), ds)
	ids.Implements((*exp.AppendableExpression)(nil), ds)
}

func (ids *insertDatasetSuite) TestClone() {
	ds := builder.Insert("test")
	ids.Equal(ds.Clone(), ds)
}

func (ids *insertDatasetSuite) TestExpression() {
	ds := builder.Insert("test")
	ids.Equal(ds.Expression(), ds)
}

func (ids *insertDatasetSuite) TestDialect() {
	ds := builder.Insert("test")
	ids.NotNil(ds.Dialect())
}

func (ids *insertDatasetSuite) TestWithDialect() {
	ds := builder.Insert("test")
	md := new(mocks.SQLDialect)
	ds = ds.SetDialect(md)

	dialect := builder.GetDialect("default")
	dialectDs := ds.WithDialect("default")
	ids.Equal(md, ds.Dialect())
	ids.Equal(dialect, dialectDs.Dialect())
}

func (ids *insertDatasetSuite) TestPrepared() {
	ds := builder.Insert("test")
	preparedDs := ds.Prepared(true)
	ids.True(preparedDs.IsPrepared())
	ids.False(ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	ids.True(preparedDs.Returning(builder.C("col")).IsPrepared())

	defer builder.SetDefaultPrepared(false)
	builder.SetDefaultPrepared(true)

	// should be prepared by default
	ds = builder.Insert("test")
	ids.True(ds.IsPrepared())
}

func (ids *insertDatasetSuite) TestGetClauses() {
	ds := builder.Insert("test")
	ce := exp.NewInsertClauses().SetInto(builder.I("test"))
	ids.Equal(ce, ds.GetClauses())
}

func (ids *insertDatasetSuite) TestWith() {
	from := builder.From("cte")
	bd := builder.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds: bd.With("test-cte", from),
			clauses: exp.NewInsertClauses().
				SetInto(builder.C("items")).
				CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from)),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestWithRecursive() {
	from := builder.From("cte")
	bd := builder.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds: bd.WithRecursive("test-cte", from),
			clauses: exp.NewInsertClauses().
				SetInto(builder.C("items")).
				CommonTablesAppend(exp.NewCommonTableExpression(true, "test-cte", from)),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestInto() {
	bd := builder.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds:      bd.Into("items2"),
			clauses: exp.NewInsertClauses().SetInto(builder.C("items2")),
		},
		insertTestCase{
			ds:      bd.Into(builder.L("items2")),
			clauses: exp.NewInsertClauses().SetInto(builder.L("items2")),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")),
		},
	)

	ids.PanicsWithValue(builder.ErrUnsupportedIntoType, func() {
		bd.Into(true)
	})
}

func (ids *insertDatasetSuite) TestCols() {
	bd := builder.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds: bd.Cols("a", "b"),
			clauses: exp.NewInsertClauses().
				SetInto(builder.C("items")).
				SetCols(exp.NewColumnListExpression("a", "b")),
		},
		insertTestCase{
			ds: bd.Cols("a", "b").Cols("c", "d"),
			clauses: exp.NewInsertClauses().
				SetInto(builder.C("items")).
				SetCols(exp.NewColumnListExpression("c", "d")),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestClearCols() {
	bd := builder.Insert("items").Cols("a", "b")
	ids.assertCases(
		insertTestCase{
			ds:      bd.ClearCols(),
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")).SetCols(exp.NewColumnListExpression("a", "b")),
		},
	)
}

func (ids *insertDatasetSuite) TestColsAppend() {
	bd := builder.Insert("items").Cols("a")
	ids.assertCases(
		insertTestCase{
			ds:      bd.ColsAppend("b"),
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")).SetCols(exp.NewColumnListExpression("a", "b")),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")).SetCols(exp.NewColumnListExpression("a")),
		},
	)
}

func (ids *insertDatasetSuite) TestFromQuery() {
	bd := builder.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds: bd.FromQuery(builder.From("other_items").Where(builder.C("b").Gt(10))),
			clauses: exp.NewInsertClauses().
				SetInto(builder.C("items")).
				SetFrom(builder.From("other_items").Where(builder.C("b").Gt(10))),
		},
		insertTestCase{
			ds: bd.FromQuery(builder.From("other_items").Where(builder.C("b").Gt(10))).Cols("a", "b"),
			clauses: exp.NewInsertClauses().
				SetInto(builder.C("items")).
				SetCols(exp.NewColumnListExpression("a", "b")).
				SetFrom(builder.From("other_items").Where(builder.C("b").Gt(10))),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestFromQueryDialectInheritance() {
	md := new(mocks.SQLDialect)
	md.On("Dialect").Return("dialect")

	ids.Run("ok, default dialect is replaced with insert dialect", func() {
		bd := builder.Insert("items").SetDialect(md).FromQuery(builder.From("other_items"))
		ids.Require().Equal(md, bd.GetClauses().From().(*builder.SelectDataset).Dialect())
	})

	ids.Run("ok, insert and select dialects coincide", func() {
		bd := builder.Insert("items").SetDialect(md).FromQuery(builder.From("other_items").SetDialect(md))
		ids.Require().Equal(md, bd.GetClauses().From().(*builder.SelectDataset).Dialect())
	})

	ids.Run("ok, insert and select dialects are default", func() {
		bd := builder.Insert("items").FromQuery(builder.From("other_items"))
		ids.Require().Equal(builder.GetDialect("default"), bd.GetClauses().From().(*builder.SelectDataset).Dialect())
	})

	ids.Run("panic, insert and select dialects are different", func() {
		defer func() {
			r := recover()
			if r == nil {
				ids.Fail("there should be a panic")
			}
			ids.Require().Equal(
				"incompatible dialects for INSERT (\"dialect\") and SELECT (\"other_dialect\")",
				r.(error).Error(),
			)
		}()

		otherDialect := new(mocks.SQLDialect)
		otherDialect.On("Dialect").Return("other_dialect")
		builder.Insert("items").SetDialect(md).FromQuery(builder.From("otherItems").SetDialect(otherDialect))
	})
}

func (ids *insertDatasetSuite) TestVals() {
	val1 := []any{
		"a", "b",
	}
	val2 := []any{
		"c", "d",
	}

	bd := builder.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds: bd.Vals(val1),
			clauses: exp.NewInsertClauses().
				SetInto(builder.C("items")).
				SetVals([][]any{val1}),
		},
		insertTestCase{
			ds: bd.Vals(val1, val2),
			clauses: exp.NewInsertClauses().
				SetInto(builder.C("items")).
				SetVals([][]any{val1, val2}),
		},
		insertTestCase{
			ds: bd.Vals(val1).Vals(val2),
			clauses: exp.NewInsertClauses().
				SetInto(builder.C("items")).
				SetVals([][]any{val1, val2}),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestClearVals() {
	val := []any{
		"a", "b",
	}
	bd := builder.Insert("items").Vals(val)
	ids.assertCases(
		insertTestCase{
			ds:      bd.ClearVals(),
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")).SetVals([][]any{val}),
		},
	)
}

func (ids *insertDatasetSuite) TestRows() {
	type item struct {
		CreatedAt *time.Time `db:"created_at"`
	}
	n := time.Now()
	r := item{CreatedAt: nil}
	r2 := item{CreatedAt: &n}
	bd := builder.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds:      bd.Rows(r),
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")).SetRows([]any{r}),
		},
		insertTestCase{
			ds:      bd.Rows(r).Rows(r2),
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")).SetRows([]any{r2}),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestClearRows() {
	type item struct {
		CreatedAt *time.Time `db:"created_at"`
	}
	r := item{CreatedAt: nil}
	bd := builder.Insert("items").Rows(r)
	ids.assertCases(
		insertTestCase{
			ds:      bd.ClearRows(),
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")).SetRows([]any{r}),
		},
	)
}

func (ids *insertDatasetSuite) TestOnConflict() {
	du := builder.DoUpdate("other_items", builder.Record{"a": 1})

	bd := builder.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds:      bd.OnConflict(nil),
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")),
		},
		insertTestCase{
			ds:      bd.OnConflict(builder.DoNothing()),
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")).SetOnConflict(builder.DoNothing()),
		},
		insertTestCase{
			ds:      bd.OnConflict(du),
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")).SetOnConflict(du),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestAs() {
	du := builder.DoUpdate("other_items", builder.Record{"new.a": 1})

	bd := builder.Insert("items").As("new")
	ids.assertCases(
		insertTestCase{
			ds: bd.OnConflict(nil),
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")).
				SetAlias(exp.NewIdentifierExpression("", "new", "")),
		},
		insertTestCase{
			ds: bd.OnConflict(builder.DoNothing()),
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")).
				SetAlias(exp.NewIdentifierExpression("", "new", "")).
				SetOnConflict(builder.DoNothing()),
		},
		insertTestCase{
			ds: bd.OnConflict(du),
			clauses: exp.NewInsertClauses().
				SetAlias(exp.NewIdentifierExpression("", "new", "")).
				SetInto(builder.C("items")).SetOnConflict(du),
		},
		insertTestCase{
			ds: bd,
			clauses: exp.NewInsertClauses().
				SetAlias(exp.NewIdentifierExpression("", "new", "")).
				SetInto(builder.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestClearOnConflict() {
	du := builder.DoUpdate("other_items", builder.Record{"a": 1})

	bd := builder.Insert("items").OnConflict(du)
	ids.assertCases(
		insertTestCase{
			ds:      bd.ClearOnConflict(),
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")).SetOnConflict(du),
		},
	)
}

func (ids *insertDatasetSuite) TestReturning() {
	bd := builder.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds: bd.Returning("a"),
			clauses: exp.NewInsertClauses().
				SetInto(builder.C("items")).
				SetReturning(exp.NewColumnListExpression("a")),
		},
		insertTestCase{
			ds: bd.Returning(),
			clauses: exp.NewInsertClauses().
				SetInto(builder.C("items")).
				SetReturning(exp.NewColumnListExpression()),
		},
		insertTestCase{
			ds: bd.Returning(nil),
			clauses: exp.NewInsertClauses().
				SetInto(builder.C("items")).
				SetReturning(exp.NewColumnListExpression()),
		},
		insertTestCase{
			ds: bd.Returning(),
			clauses: exp.NewInsertClauses().
				SetInto(builder.C("items")).
				SetReturning(exp.NewColumnListExpression()),
		},
		insertTestCase{
			ds: bd.Returning("a").Returning("b"),
			clauses: exp.NewInsertClauses().
				SetInto(builder.C("items")).
				SetReturning(exp.NewColumnListExpression("b")),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(builder.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestReturnsColumns() {
	ds := builder.Insert("test")
	ids.False(ds.ReturnsColumns())
	ids.True(ds.Returning("foo", "bar").ReturnsColumns())
}

func (ids *insertDatasetSuite) TestExec() {
	mDB, _, err := sqlmock.New()
	ids.NoError(err)

	conn := sqlx.NewSqlConnFromDB(mDB)
	ds := builder.New("mock", conn).Insert("items").
		Rows(builder.Record{"address": "111 Test Addr", "name": "Test1"})

	isql, args, err := ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1')`, isql)

	isql, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]any{"111 Test Addr", "Test1"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?)`, isql)

	defer builder.SetDefaultPrepared(false)
	builder.SetDefaultPrepared(true)

	isql, args, err = ds.ToSQL()
	ids.NoError(err)
	ids.Equal([]any{"111 Test Addr", "Test1"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?)`, isql)
}

func (ids *insertDatasetSuite) TestInsertStruct() {
	mDB, _, err := sqlmock.New()
	ids.NoError(err)

	item := dsTestActionItem{
		Address: "111 Test Addr",
		Name:    "Test1",
	}

	conn := sqlx.NewSqlConnFromDB(mDB)
	ds := builder.New("mock", conn).Insert("items").
		Rows(item)

	isql, args, err := ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1')`, isql)

	isql, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]any{"111 Test Addr", "Test1"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?)`, isql)

	isql, args, err = ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1')`, isql)

	isql, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]any{"111 Test Addr", "Test1"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?)`, isql)
}

func (ids *insertDatasetSuite) TestToSQL() {
	md := new(mocks.SQLDialect)
	ds := builder.Insert("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToInsertSQL", sqlB, c).Return(nil).Once()
	insertSQL, args, err := ds.ToSQL()
	ids.Empty(insertSQL)
	ids.Empty(args)
	ids.Nil(err)
	md.AssertExpectations(ids.T())
}

func (ids *insertDatasetSuite) TestToSQL_Prepared() {
	md := new(mocks.SQLDialect)
	ds := builder.Insert("test").SetDialect(md).Prepared(true)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(true)
	md.On("ToInsertSQL", sqlB, c).Return(nil).Once()
	insertSQL, args, err := ds.ToSQL()
	ids.Empty(insertSQL)
	ids.Empty(args)
	ids.Nil(err)
	md.AssertExpectations(ids.T())
}

func (ids *insertDatasetSuite) TestToSQL_ReturnedError() {
	md := new(mocks.SQLDialect)
	ds := builder.Insert("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	ee := errors.New("expected error")
	md.On("ToInsertSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	insertSQL, args, err := ds.ToSQL()
	ids.Empty(insertSQL)
	ids.Empty(args)
	ids.Equal(ee, err)
	md.AssertExpectations(ids.T())
}

func (ids *insertDatasetSuite) TestSetError() {
	err1 := errors.New("error #1")
	err2 := errors.New("error #2")
	err3 := errors.New("error #3")

	// Verify initial error set/get works properly
	md := new(mocks.SQLDialect)
	ds := builder.Insert("test").SetDialect(md)
	ds = ds.SetError(err1)
	ids.Equal(err1, ds.Error())
	sql, args, err := ds.ToSQL()
	ids.Empty(sql)
	ids.Empty(args)
	ids.Equal(err1, err)

	// Repeated SetError calls on Dataset should not overwrite the original error
	ds = ds.SetError(err2)
	ids.Equal(err1, ds.Error())
	sql, args, err = ds.ToSQL()
	ids.Empty(sql)
	ids.Empty(args)
	ids.Equal(err1, err)

	// Builder functions should not lose the error
	ds = ds.Cols("a", "b")
	ids.Equal(err1, ds.Error())
	sql, args, err = ds.ToSQL()
	ids.Empty(sql)
	ids.Empty(args)
	ids.Equal(err1, err)

	// Deeper errors inside SQL generation should still return original error
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToInsertSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(err3)
	}).Once()

	sql, args, err = ds.ToSQL()
	ids.Empty(sql)
	ids.Empty(args)
	ids.Equal(err1, err)
}

func TestInsertDataset(t *testing.T) {
	suite.Run(t, new(insertDatasetSuite))
}
