package builder_test

import (
	"testing"

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
	deleteTestCase struct {
		ds      *builder.DeleteDataset
		clauses exp.DeleteClauses
	}
	deleteDatasetSuite struct {
		suite.Suite
	}
)

func (dds *deleteDatasetSuite) assertCases(cases ...deleteTestCase) {
	for _, s := range cases {
		dds.Equal(s.clauses, s.ds.GetClauses())
	}
}

func (dds *deleteDatasetSuite) SetupSuite() {
	noReturn := builder.DefaultDialectOptions()
	noReturn.SupportsReturn = false
	builder.RegisterDialect("no-return", noReturn)

	limitOnDelete := builder.DefaultDialectOptions()
	limitOnDelete.SupportsLimitOnDelete = true
	builder.RegisterDialect("limit-on-delete", limitOnDelete)

	orderOnDelete := builder.DefaultDialectOptions()
	orderOnDelete.SupportsOrderByOnDelete = true
	builder.RegisterDialect("order-on-delete", orderOnDelete)
}

func (dds *deleteDatasetSuite) TearDownSuite() {
	builder.DeregisterDialect("no-return")
	builder.DeregisterDialect("limit-on-delete")
	builder.DeregisterDialect("order-on-delete")
}

func (dds *deleteDatasetSuite) TestDelete() {
	ds := builder.Delete("test")
	dds.IsType(&builder.DeleteDataset{}, ds)
	dds.Implements((*exp.Expression)(nil), ds)
	dds.Implements((*exp.AppendableExpression)(nil), ds)
}

func (dds *deleteDatasetSuite) TestClone() {
	ds := builder.Delete("test")
	dds.Equal(ds.Clone(), ds)
}

func (dds *deleteDatasetSuite) TestExpression() {
	ds := builder.Delete("test")
	dds.Equal(ds.Expression(), ds)
}

func (dds *deleteDatasetSuite) TestDialect() {
	ds := builder.Delete("test")
	dds.NotNil(ds.Dialect())
}

func (dds *deleteDatasetSuite) TestWithDialect() {
	ds := builder.Delete("test")
	md := new(mocks.SQLDialect)
	ds = ds.SetDialect(md)

	dialect := builder.GetDialect("default")
	dialectDs := ds.WithDialect("default")
	dds.Equal(md, ds.Dialect())
	dds.Equal(dialect, dialectDs.Dialect())
}

func (dds *deleteDatasetSuite) TestPrepared() {
	ds := builder.Delete("test")
	preparedDs := ds.Prepared(true)
	dds.True(preparedDs.IsPrepared())
	dds.False(ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	dds.True(preparedDs.Where(builder.Ex{"a": 1}).IsPrepared())

	defer builder.SetDefaultPrepared(false)
	builder.SetDefaultPrepared(true)

	// should be prepared by default
	ds = builder.Delete("test")
	dds.True(ds.IsPrepared())
}

func (dds *deleteDatasetSuite) TestGetClauses() {
	ds := builder.Delete("test")
	ce := exp.NewDeleteClauses().SetFrom(builder.I("test"))
	dds.Equal(ce, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestWith() {
	from := builder.From("cte")
	bd := builder.Delete("items")
	dds.assertCases(
		deleteTestCase{
			ds: bd.With("test-cte", from),
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("items")).
				CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from)),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("items")),
		},
	)
}

func (dds *deleteDatasetSuite) TestWithRecursive() {
	from := builder.From("cte")
	bd := builder.Delete("items")
	dds.assertCases(
		deleteTestCase{
			ds: bd.WithRecursive("test-cte", from),
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("items")).
				CommonTablesAppend(exp.NewCommonTableExpression(true, "test-cte", from)),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("items")),
		},
	)
}

func (dds *deleteDatasetSuite) TestFrom_withIdentifier() {
	bd := builder.Delete("items")
	dds.assertCases(
		deleteTestCase{
			ds:      bd.From("items2"),
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("items2")),
		},
		deleteTestCase{
			ds:      bd.From(builder.C("items2")),
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("items2")),
		},
		deleteTestCase{
			ds:      bd.From(builder.T("items2")),
			clauses: exp.NewDeleteClauses().SetFrom(builder.T("items2")),
		},
		deleteTestCase{
			ds:      bd.From("schema.table"),
			clauses: exp.NewDeleteClauses().SetFrom(builder.I("schema.table")),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("items")),
		},
	)

	dds.PanicsWithValue(builder.ErrBadFromArgument, func() {
		builder.Delete("test").From(true)
	})
}

func (dds *deleteDatasetSuite) TestWhere() {
	bd := builder.Delete("items")
	dds.assertCases(
		deleteTestCase{
			ds: bd.Where(builder.Ex{"a": 1}),
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("items")).
				WhereAppend(builder.Ex{"a": 1}),
		},
		deleteTestCase{
			ds: bd.Where(builder.Ex{"a": 1}).Where(builder.C("b").Eq("c")),
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("items")).
				WhereAppend(builder.Ex{"a": 1}).
				WhereAppend(builder.C("b").Eq("c")),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("items")),
		},
	)
}

func (dds *deleteDatasetSuite) TestClearWhere() {
	bd := builder.Delete("items").Where(builder.Ex{"a": 1})
	dds.assertCases(
		deleteTestCase{
			ds: bd.ClearWhere(),
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("items")),
		},
		deleteTestCase{
			ds: bd,
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("items")).
				WhereAppend(builder.Ex{"a": 1}),
		},
	)
}

func (dds *deleteDatasetSuite) TestOrder() {
	bd := builder.Delete("items")
	dds.assertCases(
		deleteTestCase{
			ds: bd.Order(builder.C("a").Asc()),
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("items")).
				SetOrder(builder.C("a").Asc()),
		},
		deleteTestCase{
			ds: bd.Order(builder.C("a").Asc()).Order(builder.C("b").Desc()),
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("items")).
				SetOrder(builder.C("b").Desc()),
		},
		deleteTestCase{
			ds: bd.Order(builder.C("a").Asc(), builder.C("b").Desc()),
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("items")).
				SetOrder(builder.C("a").Asc(), builder.C("b").Desc()),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("items")),
		},
	)
}

func (dds *deleteDatasetSuite) TestOrderAppend() {
	bd := builder.Delete("items").Order(builder.C("a").Asc())
	dds.assertCases(
		deleteTestCase{
			ds: bd.OrderAppend(builder.C("b").Desc()),
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("items")).
				SetOrder(builder.C("a").Asc(), builder.C("b").Desc()),
		},
		deleteTestCase{
			ds: bd,
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("items")).
				SetOrder(builder.C("a").Asc()),
		},
	)
}

func (dds *deleteDatasetSuite) TestOrderPrepend() {
	bd := builder.Delete("items").Order(builder.C("a").Asc())
	dds.assertCases(
		deleteTestCase{
			ds: bd.OrderPrepend(builder.C("b").Desc()),
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("items")).
				SetOrder(builder.C("b").Desc(), builder.C("a").Asc()),
		},
		deleteTestCase{
			ds: bd,
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("items")).
				SetOrder(builder.C("a").Asc()),
		},
	)
}

func (dds *deleteDatasetSuite) TestClearOrder() {
	bd := builder.Delete("items").Order(builder.C("a").Asc())
	dds.assertCases(
		deleteTestCase{
			ds:      bd.ClearOrder(),
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("items")),
		},
		deleteTestCase{
			ds: bd,
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("items")).
				SetOrder(builder.C("a").Asc()),
		},
	)
}

func (dds *deleteDatasetSuite) TestLimit() {
	bd := builder.Delete("test")
	dds.assertCases(
		deleteTestCase{
			ds: bd.Limit(10),
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("test")).
				SetLimit(uint(10)),
		},
		deleteTestCase{
			ds:      bd.Limit(0),
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("test")),
		},
		deleteTestCase{
			ds: bd.Limit(10).Limit(2),
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("test")).
				SetLimit(uint(2)),
		},
		deleteTestCase{
			ds:      bd.Limit(10).Limit(0),
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("test")),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("test")),
		},
	)
}

func (dds *deleteDatasetSuite) TestLimitAll() {
	bd := builder.Delete("test")
	dds.assertCases(
		deleteTestCase{
			ds: bd.LimitAll(),
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("test")).
				SetLimit(builder.L("ALL")),
		},
		deleteTestCase{
			ds: bd.Limit(10).LimitAll(),
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("test")).
				SetLimit(builder.L("ALL")),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("test")),
		},
	)
}

func (dds *deleteDatasetSuite) TestClearLimit() {
	bd := builder.Delete("test").Limit(10)
	dds.assertCases(
		deleteTestCase{
			ds:      bd.ClearLimit(),
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("test")),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("test")).SetLimit(uint(10)),
		},
	)
}

func (dds *deleteDatasetSuite) TestReturning() {
	bd := builder.Delete("items")
	dds.assertCases(
		deleteTestCase{
			ds: bd.Returning("a"),
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("items")).
				SetReturning(exp.NewColumnListExpression("a")),
		},
		deleteTestCase{
			ds: bd.Returning(),
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("items")).
				SetReturning(exp.NewColumnListExpression()),
		},
		deleteTestCase{
			ds: bd.Returning(nil),
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("items")).
				SetReturning(exp.NewColumnListExpression()),
		},
		deleteTestCase{
			ds: bd.Returning("a").Returning("b"),
			clauses: exp.NewDeleteClauses().
				SetFrom(builder.C("items")).
				SetReturning(exp.NewColumnListExpression("b")),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(builder.C("items")),
		},
	)
}

func (dds *deleteDatasetSuite) TestReturnsColumns() {
	ds := builder.Delete("test")
	dds.False(ds.ReturnsColumns())
	dds.True(ds.Returning("foo", "bar").ReturnsColumns())
}

func (dds *deleteDatasetSuite) TestToSQL() {
	md := new(mocks.SQLDialect)
	ds := builder.Delete("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToDeleteSQL", sqlB, c).Return(nil).Once()

	sql, args, err := ds.ToSQL()
	dds.Empty(sql)
	dds.Empty(args)
	dds.Nil(err)
	md.AssertExpectations(dds.T())
}

func (dds *deleteDatasetSuite) TestToSQL_Prepared() {
	md := new(mocks.SQLDialect)
	ds := builder.Delete("test").Prepared(true).SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(true)
	md.On("ToDeleteSQL", sqlB, c).Return(nil).Once()

	sql, args, err := ds.ToSQL()
	dds.Empty(sql)
	dds.Empty(args)
	dds.Nil(err)
	md.AssertExpectations(dds.T())
}

func (dds *deleteDatasetSuite) TestToSQL_WithError() {
	md := new(mocks.SQLDialect)
	ds := builder.Delete("test").SetDialect(md)
	c := ds.GetClauses()
	ee := errors.New("expected error")
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToDeleteSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	sql, args, err := ds.ToSQL()
	dds.Empty(sql)
	dds.Empty(args)
	dds.Equal(ee, err)
	md.AssertExpectations(dds.T())
}

func (dds *deleteDatasetSuite) TestExecutor() {
	mDB, _, err := sqlmock.New()
	dds.NoError(err)

	conn := sqlx.NewSqlConnFromDB(mDB)
	ds := builder.New("mock", conn).Delete("items").Where(builder.Ex{"id": builder.Op{"gt": 10}})

	dsql, args, err := ds.ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(`DELETE FROM "items" WHERE ("id" > 10)`, dsql)

	dsql, args, err = ds.Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Equal([]any{int64(10)}, args)
	dds.Equal(`DELETE FROM "items" WHERE ("id" > ?)`, dsql)

	defer builder.SetDefaultPrepared(false)
	builder.SetDefaultPrepared(true)

	dsql, args, err = ds.ToSQL()
	dds.NoError(err)
	dds.Equal([]any{int64(10)}, args)
	dds.Equal(`DELETE FROM "items" WHERE ("id" > ?)`, dsql)
}

func (dds *deleteDatasetSuite) TestSetError() {
	err1 := errors.New("error #1")
	err2 := errors.New("error #2")
	err3 := errors.New("error #3")

	// Verify initial error set/get works properly
	md := new(mocks.SQLDialect)
	ds := builder.Delete("test").SetDialect(md)
	ds = ds.SetError(err1)
	dds.Equal(err1, ds.Error())
	sql, args, err := ds.ToSQL()
	dds.Empty(sql)
	dds.Empty(args)
	dds.Equal(err1, err)

	// Repeated SetError calls on Dataset should not overwrite the original error
	ds = ds.SetError(err2)
	dds.Equal(err1, ds.Error())
	sql, args, err = ds.ToSQL()
	dds.Empty(sql)
	dds.Empty(args)
	dds.Equal(err1, err)

	// Builder functions should not lose the error
	ds = ds.ClearLimit()
	dds.Equal(err1, ds.Error())
	sql, args, err = ds.ToSQL()
	dds.Empty(sql)
	dds.Empty(args)
	dds.Equal(err1, err)

	// Deeper errors inside SQL generation should still return original error
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToDeleteSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(err3)
	}).Once()

	sql, args, err = ds.ToSQL()
	dds.Empty(sql)
	dds.Empty(args)
	dds.Equal(err1, err)
}

func TestDeleteDataset(t *testing.T) {
	suite.Run(t, new(deleteDatasetSuite))
}
