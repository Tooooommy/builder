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
	updateTestCase struct {
		ds      *builder.UpdateDataset
		clauses exp.UpdateClauses
	}
	updateDatasetSuite struct {
		suite.Suite
	}
)

func (uds *updateDatasetSuite) assertCases(cases ...updateTestCase) {
	for _, s := range cases {
		uds.Equal(s.clauses, s.ds.GetClauses())
	}
}

func (uds *updateDatasetSuite) TestUpdate() {
	ds := builder.Update("test")
	uds.IsType(&builder.UpdateDataset{}, ds)
	uds.Implements((*exp.Expression)(nil), ds)
	uds.Implements((*exp.AppendableExpression)(nil), ds)
}

func (uds *updateDatasetSuite) TestClone() {
	ds := builder.Update("test")
	uds.Equal(ds, ds.Clone())
}

func (uds *updateDatasetSuite) TestExpression() {
	ds := builder.Update("test")
	uds.Equal(ds, ds.Expression())
}

func (uds *updateDatasetSuite) TestDialect() {
	ds := builder.Update("test")
	uds.NotNil(ds.Dialect())
}

func (uds *updateDatasetSuite) TestWithDialect() {
	ds := builder.Update("test")
	md := new(mocks.SQLDialect)
	ds = ds.SetDialect(md)

	dialect := builder.GetDialect("default")
	dialectDs := ds.WithDialect("default")
	uds.Equal(md, ds.Dialect())
	uds.Equal(dialect, dialectDs.Dialect())
}

func (uds *updateDatasetSuite) TestPrepared() {
	ds := builder.Update("test")
	preparedDs := ds.Prepared(true)
	uds.True(preparedDs.IsPrepared())
	uds.False(ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	uds.True(preparedDs.Where(builder.Ex{"a": 1}).IsPrepared())

	defer builder.SetDefaultPrepared(false)
	builder.SetDefaultPrepared(true)

	// should be prepared by default
	ds = builder.Update("test")
	uds.True(ds.IsPrepared())
}

func (uds *updateDatasetSuite) TestGetClauses() {
	ds := builder.Update("test")
	ce := exp.NewUpdateClauses().SetTable(builder.I("test"))
	uds.Equal(ce, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestWith() {
	from := builder.Update("cte")
	bd := builder.Update("items")
	uds.assertCases(
		updateTestCase{
			ds: bd.With("test-cte", from),
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from)),
		},
		updateTestCase{
			ds:      bd,
			clauses: exp.NewUpdateClauses().SetTable(builder.C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestWithRecursive() {
	from := builder.Update("cte")
	bd := builder.Update("items")
	uds.assertCases(
		updateTestCase{
			ds: bd.WithRecursive("test-cte", from),
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				CommonTablesAppend(exp.NewCommonTableExpression(true, "test-cte", from)),
		},
		updateTestCase{
			ds:      bd,
			clauses: exp.NewUpdateClauses().SetTable(builder.C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestTable() {
	bd := builder.Update("items")
	uds.assertCases(
		updateTestCase{
			ds:      bd.Table("items2"),
			clauses: exp.NewUpdateClauses().SetTable(builder.C("items2")),
		},
		updateTestCase{
			ds:      bd.Table(builder.L("literal_table")),
			clauses: exp.NewUpdateClauses().SetTable(builder.L("literal_table")),
		},
		updateTestCase{
			ds:      bd,
			clauses: exp.NewUpdateClauses().SetTable(builder.C("items")),
		},
	)
	uds.PanicsWithValue(builder.ErrUnsupportedUpdateTableType, func() {
		bd.Table(true)
	})
}

func (uds *updateDatasetSuite) TestSet() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	bd := builder.Update("items")
	uds.assertCases(
		updateTestCase{
			ds: bd.Set(item{Name: "Test", Address: "111 Test Addr"}),
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				SetSetValues(item{Name: "Test", Address: "111 Test Addr"}),
		},
		updateTestCase{
			ds: bd.Set(builder.Record{"name": "Test", "address": "111 Test Addr"}),
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				SetSetValues(builder.Record{"name": "Test", "address": "111 Test Addr"}),
		},
		updateTestCase{
			ds: bd,
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestFrom() {
	bd := builder.Update("items")
	uds.assertCases(
		updateTestCase{
			ds: bd.From("other"),
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				SetFrom(exp.NewColumnListExpression("other")),
		},
		updateTestCase{
			ds: bd.From("other").From("other2"),
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				SetFrom(exp.NewColumnListExpression("other2")),
		},
		updateTestCase{
			ds: bd,
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestWhere() {
	bd := builder.Update("items")
	uds.assertCases(
		updateTestCase{
			ds: bd.Where(builder.Ex{"a": 1}),
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				WhereAppend(builder.Ex{"a": 1}),
		},
		updateTestCase{
			ds: bd.Where(builder.Ex{"a": 1}).Where(builder.C("b").Eq("c")),
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				WhereAppend(builder.Ex{"a": 1}).WhereAppend(builder.C("b").Eq("c")),
		},
		updateTestCase{
			ds: bd,
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestClearWhere() {
	bd := builder.Update("items").Where(builder.Ex{"a": 1})
	uds.assertCases(
		updateTestCase{
			ds:      bd.ClearWhere(),
			clauses: exp.NewUpdateClauses().SetTable(builder.C("items")),
		},
		updateTestCase{
			ds: bd,
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				WhereAppend(builder.Ex{"a": 1}),
		},
	)
}

func (uds *updateDatasetSuite) TestOrder() {
	bd := builder.Update("items")
	uds.assertCases(
		updateTestCase{
			ds: bd.Order(builder.C("a").Desc()),
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).OrderAppend(builder.C("a").Desc()),
		},
		updateTestCase{
			ds: bd.Order(builder.C("a").Desc()).Order(builder.C("b").Asc()),
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				OrderAppend(builder.C("b").Asc()),
		},
		updateTestCase{
			ds:      bd,
			clauses: exp.NewUpdateClauses().SetTable(builder.C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestOrderAppend() {
	bd := builder.Update("items").Order(builder.C("a").Desc())
	uds.assertCases(
		updateTestCase{
			ds: bd.OrderAppend(builder.C("b").Asc()),
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				OrderAppend(builder.C("a").Desc()).
				OrderAppend(builder.C("b").Asc()),
		},
		updateTestCase{
			ds: bd,
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				OrderAppend(builder.C("a").Desc()),
		},
	)
}

func (uds *updateDatasetSuite) TestOrderPrepend() {
	bd := builder.Update("items").Order(builder.C("a").Desc())
	uds.assertCases(
		updateTestCase{
			ds: bd.OrderPrepend(builder.C("b").Asc()),
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				OrderAppend(builder.C("b").Asc()).
				OrderAppend(builder.C("a").Desc()),
		},
		updateTestCase{
			ds: bd,
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				OrderAppend(builder.C("a").Desc()),
		},
	)
}

func (uds *updateDatasetSuite) TestClearOrder() {
	bd := builder.Update("items").Order(builder.C("a").Desc())
	uds.assertCases(
		updateTestCase{
			ds:      bd.ClearOrder(),
			clauses: exp.NewUpdateClauses().SetTable(builder.C("items")),
		},
		updateTestCase{
			ds: bd,
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				OrderAppend(builder.C("a").Desc()),
		},
	)
}

func (uds *updateDatasetSuite) TestLimit() {
	bd := builder.Update("items")
	uds.assertCases(
		updateTestCase{
			ds:      bd.Limit(10),
			clauses: exp.NewUpdateClauses().SetTable(builder.C("items")).SetLimit(uint(10)),
		},
		updateTestCase{
			ds:      bd.Limit(0),
			clauses: exp.NewUpdateClauses().SetTable(builder.C("items")),
		},
		updateTestCase{
			ds:      bd,
			clauses: exp.NewUpdateClauses().SetTable(builder.C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestLimitAll() {
	bd := builder.Update("items")
	uds.assertCases(
		updateTestCase{
			ds:      bd.LimitAll(),
			clauses: exp.NewUpdateClauses().SetTable(builder.C("items")).SetLimit(builder.L("ALL")),
		},
		updateTestCase{
			ds:      bd,
			clauses: exp.NewUpdateClauses().SetTable(builder.C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestClearLimit() {
	bd := builder.Update("items")
	uds.assertCases(
		updateTestCase{
			ds:      bd.LimitAll().ClearLimit(),
			clauses: exp.NewUpdateClauses().SetTable(builder.C("items")),
		},
		updateTestCase{
			ds:      bd.Limit(10).ClearLimit(),
			clauses: exp.NewUpdateClauses().SetTable(builder.C("items")),
		},
		updateTestCase{
			ds:      bd,
			clauses: exp.NewUpdateClauses().SetTable(builder.C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestReturning() {
	bd := builder.Update("items")
	uds.assertCases(
		updateTestCase{
			ds: bd.Returning("a", "b"),
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				SetReturning(exp.NewColumnListExpression("a", "b")),
		},
		updateTestCase{
			ds: bd.Returning(),
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				SetReturning(exp.NewColumnListExpression()),
		},
		updateTestCase{
			ds: bd.Returning(nil),
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				SetReturning(exp.NewColumnListExpression()),
		},
		updateTestCase{
			ds: bd.Returning("a", "b").Returning("c"),
			clauses: exp.NewUpdateClauses().
				SetTable(builder.C("items")).
				SetReturning(exp.NewColumnListExpression("c")),
		},
		updateTestCase{
			ds:      bd,
			clauses: exp.NewUpdateClauses().SetTable(builder.C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestReturnsColumns() {
	ds := builder.Update("test")
	uds.False(ds.ReturnsColumns())
	uds.True(ds.Returning("foo", "bar").ReturnsColumns())
}

func (uds *updateDatasetSuite) TestToSQL() {
	md := new(mocks.SQLDialect)
	ds := builder.Update("test").SetDialect(md)
	r := builder.Record{"c": "a"}
	c := ds.GetClauses().SetSetValues(r)
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToUpdateSQL", sqlB, c).Return(nil).Once()
	updateSQL, args, err := ds.Set(r).ToSQL()
	uds.Empty(updateSQL)
	uds.Empty(args)
	uds.Nil(err)
	md.AssertExpectations(uds.T())
}

func (uds *updateDatasetSuite) TestToSQL_Prepared() {
	md := new(mocks.SQLDialect)
	ds := builder.Update("test").Prepared(true).SetDialect(md)
	r := builder.Record{"c": "a"}
	c := ds.GetClauses().SetSetValues(r)
	sqlB := sb.NewSQLBuilder(true)
	md.On("ToUpdateSQL", sqlB, c).Return(nil).Once()
	updateSQL, args, err := ds.Set(builder.Record{"c": "a"}).ToSQL()
	uds.Empty(updateSQL)
	uds.Empty(args)
	uds.Nil(err)
	md.AssertExpectations(uds.T())
}

func (uds *updateDatasetSuite) TestToSQL_WithError() {
	md := new(mocks.SQLDialect)
	ds := builder.Update("test").SetDialect(md)
	r := builder.Record{"c": "a"}
	c := ds.GetClauses().SetSetValues(r)
	sqlB := sb.NewSQLBuilder(false)
	ee := errors.New("expected error")
	md.On("ToUpdateSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	updateSQL, args, err := ds.Set(builder.Record{"c": "a"}).ToSQL()
	uds.Empty(updateSQL)
	uds.Empty(args)
	uds.Equal(ee, err)
	md.AssertExpectations(uds.T())
}

func (uds *updateDatasetSuite) TestExec() {
	mDB, _, err := sqlmock.New()
	uds.NoError(err)
	conn := sqlx.NewSqlConnFromDB(mDB)
	ds := builder.New("mock", conn).
		Update("items").
		Set(builder.Record{"address": "111 Test Addr", "name": "Test1"}).
		Where(builder.C("name").IsNull())

	updateSQL, args, err := ds.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`, updateSQL)

	updateSQL, args, err = ds.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]any{"111 Test Addr", "Test1"}, args)
	uds.Equal(`UPDATE "items" SET "address"=?,"name"=? WHERE ("name" IS NULL)`, updateSQL)

	defer builder.SetDefaultPrepared(false)
	builder.SetDefaultPrepared(true)

	updateSQL, args, err = ds.ToSQL()
	uds.NoError(err)
	uds.Equal([]any{"111 Test Addr", "Test1"}, args)
	uds.Equal(`UPDATE "items" SET "address"=?,"name"=? WHERE ("name" IS NULL)`, updateSQL)
}

func (uds *updateDatasetSuite) TestSetError() {
	err1 := errors.New("error #1")
	err2 := errors.New("error #2")
	err3 := errors.New("error #3")

	// Verify initial error set/get works properly
	md := new(mocks.SQLDialect)
	ds := builder.Update("test").SetDialect(md)
	ds = ds.SetError(err1)
	uds.Equal(err1, ds.Error())
	sql, args, err := ds.ToSQL()
	uds.Empty(sql)
	uds.Empty(args)
	uds.Equal(err1, err)

	// Repeated SetError calls on Dataset should not overwrite the original error
	ds = ds.SetError(err2)
	uds.Equal(err1, ds.Error())
	sql, args, err = ds.ToSQL()
	uds.Empty(sql)
	uds.Empty(args)
	uds.Equal(err1, err)

	// Builder functions should not lose the error
	ds = ds.ClearLimit()
	uds.Equal(err1, ds.Error())
	sql, args, err = ds.ToSQL()
	uds.Empty(sql)
	uds.Empty(args)
	uds.Equal(err1, err)

	// Deeper errors inside SQL generation should still return original error
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToUpdateSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(err3)
	}).Once()

	sql, args, err = ds.ToSQL()
	uds.Empty(sql)
	uds.Empty(args)
	uds.Equal(err1, err)
}

func TestUpdateDataset(t *testing.T) {
	suite.Run(t, new(updateDatasetSuite))
}
