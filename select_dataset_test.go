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
	selectTestCase struct {
		ds      *builder.SelectDataset
		clauses exp.SelectClauses
	}
	dsTestActionItem struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	selectDatasetSuite struct {
		suite.Suite
	}
)

func GenMock(number int, fn func()) {
	for i := 0; i < number; i++ {
		fn()
	}
}

func (sds *selectDatasetSuite) assertCases(cases ...selectTestCase) {
	for _, s := range cases {
		sds.Equal(s.clauses, s.ds.GetClauses())
	}
}

func (sds *selectDatasetSuite) TestReturnsColumns() {
	ds := builder.Select(builder.L("NOW()"))
	sds.True(ds.ReturnsColumns())
}

func (sds *selectDatasetSuite) TestClone() {
	ds := builder.From("test")
	sds.Equal(ds, ds.Clone())
}

func (sds *selectDatasetSuite) TestExpression() {
	ds := builder.From("test")
	sds.Equal(ds, ds.Expression())
}

func (sds *selectDatasetSuite) TestDialect() {
	ds := builder.From("test")
	sds.NotNil(ds.Dialect())
}

func (sds *selectDatasetSuite) TestWithDialect() {
	ds := builder.From("test")
	md := new(mocks.SQLDialect)
	ds = ds.SetDialect(md)

	dialect := builder.GetDialect("default")
	dialectDs := ds.WithDialect("default")
	sds.Equal(md, ds.Dialect())
	sds.Equal(dialect, dialectDs.Dialect())
}

func (sds *selectDatasetSuite) TestPrepared() {
	ds := builder.From("test")
	preparedDs := ds.Prepared(true)
	sds.True(preparedDs.IsPrepared())
	sds.False(ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	sds.True(preparedDs.Where(builder.Ex{"a": 1}).IsPrepared())

	defer builder.SetDefaultPrepared(false)
	builder.SetDefaultPrepared(true)

	// should be prepared by default
	ds = builder.From("test")
	sds.True(ds.IsPrepared())
}

func (sds *selectDatasetSuite) TestGetClauses() {
	ds := builder.From("test")
	ce := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression(builder.I("test")))
	sds.Equal(ce, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestUpdate() {
	where := builder.Ex{"a": 1}
	from := builder.From("cte")
	limit := uint(1)
	order := []exp.OrderedExpression{builder.C("a").Asc(), builder.C("b").Desc()}
	ds := builder.From("test").
		With("test-cte", from).
		Where(where).
		Limit(limit).
		Order(order...)
	ec := exp.NewUpdateClauses().
		SetTable(builder.C("test")).
		CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from)).
		WhereAppend(ds.GetClauses().Where()).
		SetLimit(limit).
		SetOrder(order...)
	sds.Equal(ec, ds.Update().GetClauses())
}

func (sds *selectDatasetSuite) TestInsert() {
	where := builder.Ex{"a": 1}
	from := builder.From("cte")
	limit := uint(1)
	order := []exp.OrderedExpression{builder.C("a").Asc(), builder.C("b").Desc()}
	ds := builder.From("test").
		With("test-cte", from).
		Where(where).
		Limit(limit).
		Order(order...)
	ec := exp.NewInsertClauses().
		SetInto(builder.C("test")).
		CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from))
	sds.Equal(ec, ds.Insert().GetClauses())
}

func (sds *selectDatasetSuite) TestDelete() {
	where := builder.Ex{"a": 1}
	from := builder.From("cte")
	limit := uint(1)
	order := []exp.OrderedExpression{builder.C("a").Asc(), builder.C("b").Desc()}
	ds := builder.From("test").
		With("test-cte", from).
		Where(where).
		Limit(limit).
		Order(order...)
	ec := exp.NewDeleteClauses().
		SetFrom(builder.C("test")).
		CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from)).
		WhereAppend(ds.GetClauses().Where()).
		SetLimit(limit).
		SetOrder(order...)
	sds.Equal(ec, ds.Delete().GetClauses())
}

func (sds *selectDatasetSuite) TestTruncate() {
	where := builder.Ex{"a": 1}
	from := builder.From("cte")
	limit := uint(1)
	order := []exp.OrderedExpression{builder.C("a").Asc(), builder.C("b").Desc()}
	ds := builder.From("test").
		With("test-cte", from).
		Where(where).
		Limit(limit).
		Order(order...)
	ec := exp.NewTruncateClauses().
		SetTable(exp.NewColumnListExpression("test"))
	sds.Equal(ec, ds.Truncate().GetClauses())
}

func (sds *selectDatasetSuite) TestWith() {
	from := builder.From("cte")
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.With("test-cte", from),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from)),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestWithRecursive() {
	from := builder.From("cte")
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.WithRecursive("test-cte", from),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				CommonTablesAppend(exp.NewCommonTableExpression(true, "test-cte", from)),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestSelect() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.Select("a", "b"),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetSelect(exp.NewColumnListExpression("a", "b")),
		},
		selectTestCase{
			ds: bd.Select("a").Select("b"),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetSelect(exp.NewColumnListExpression("b")),
		},
		selectTestCase{
			ds: bd.Select("a").Select(),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestSelectDistinct() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.SelectDistinct("a", "b"),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetSelect(exp.NewColumnListExpression("a", "b")).
				SetDistinct(exp.NewColumnListExpression()),
		},
		selectTestCase{
			ds: bd.SelectDistinct("a").SelectDistinct("b"),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetSelect(exp.NewColumnListExpression("b")).
				SetDistinct(exp.NewColumnListExpression()),
		},
		selectTestCase{
			ds: bd.Select("a").SelectDistinct("b"),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetSelect(exp.NewColumnListExpression("b")).
				SetDistinct(exp.NewColumnListExpression()),
		},
		selectTestCase{
			ds: bd.Select("a").SelectDistinct(),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetSelect(exp.NewColumnListExpression(builder.Star())).
				SetDistinct(nil),
		},
		selectTestCase{
			ds: bd.SelectDistinct("a").SelectDistinct(),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetSelect(exp.NewColumnListExpression(builder.Star())).
				SetDistinct(nil),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestClearSelect() {
	bd := builder.From("test").Select("a")
	sds.assertCases(
		selectTestCase{
			ds: bd.ClearSelect(),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")),
		},
		selectTestCase{
			ds: bd,
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetSelect(exp.NewColumnListExpression("a")),
		},
	)
}

func (sds *selectDatasetSuite) TestSelectAppend() {
	bd := builder.From("test").Select("a")
	sds.assertCases(
		selectTestCase{
			ds: bd.SelectAppend("b"),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetSelect(exp.NewColumnListExpression("a", "b")),
		},
		selectTestCase{
			ds: bd,
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetSelect(exp.NewColumnListExpression("a")),
		},
	)
}

func (sds *selectDatasetSuite) TestDistinct() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.Distinct("a", "b"),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetDistinct(exp.NewColumnListExpression("a", "b")),
		},
		selectTestCase{
			ds: bd.Distinct("a").Distinct("b"),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetDistinct(exp.NewColumnListExpression("b")),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestFrom() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.From(builder.T("test2")),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression(builder.T("test2"))),
		},
		selectTestCase{
			ds: bd.From(builder.From("test")),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression(builder.From("test").As("t1"))),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestFromSelf() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.FromSelf(),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression(bd.As("t1"))),
		},
		selectTestCase{
			ds: bd.As("alias").FromSelf(),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression(bd.As("alias"))),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestCompoundFromSelf() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds:      bd.CompoundFromSelf(),
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
		selectTestCase{
			ds:      bd.Limit(10).CompoundFromSelf(),
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression(bd.Limit(10).As("t1"))),
		},
		selectTestCase{
			ds: bd.Order(builder.C("a").Asc()).CompoundFromSelf(),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression(bd.Order(builder.C("a").Asc()).As("t1"))),
		},
		selectTestCase{
			ds: bd.As("alias").FromSelf(),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression(bd.As("alias"))),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestJoin() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.Join(builder.T("foo"), builder.On(builder.C("a").IsNull())),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				JoinsAppend(
					exp.NewConditionedJoinExpression(exp.InnerJoinType, builder.T("foo"), builder.On(builder.C("a").IsNull())),
				),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestInnerJoin() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.InnerJoin(builder.T("foo"), builder.On(builder.C("a").IsNull())),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				JoinsAppend(
					exp.NewConditionedJoinExpression(exp.InnerJoinType, builder.T("foo"), builder.On(builder.C("a").IsNull())),
				),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestFullOuterJoin() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.FullOuterJoin(builder.T("foo"), builder.On(builder.C("a").IsNull())),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				JoinsAppend(
					exp.NewConditionedJoinExpression(exp.FullOuterJoinType, builder.T("foo"), builder.On(builder.C("a").IsNull())),
				),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestRightOuterJoin() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.RightOuterJoin(builder.T("foo"), builder.On(builder.C("a").IsNull())),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				JoinsAppend(
					exp.NewConditionedJoinExpression(exp.RightOuterJoinType, builder.T("foo"), builder.On(builder.C("a").IsNull())),
				),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestLeftOuterJoin() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.LeftOuterJoin(builder.T("foo"), builder.On(builder.C("a").IsNull())),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				JoinsAppend(
					exp.NewConditionedJoinExpression(exp.LeftOuterJoinType, builder.T("foo"), builder.On(builder.C("a").IsNull())),
				),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestFullJoin() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.FullJoin(builder.T("foo"), builder.On(builder.C("a").IsNull())),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				JoinsAppend(
					exp.NewConditionedJoinExpression(exp.FullJoinType, builder.T("foo"), builder.On(builder.C("a").IsNull())),
				),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestRightJoin() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.RightJoin(builder.T("foo"), builder.On(builder.C("a").IsNull())),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				JoinsAppend(
					exp.NewConditionedJoinExpression(exp.RightJoinType, builder.T("foo"), builder.On(builder.C("a").IsNull())),
				),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestLeftJoin() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.LeftJoin(builder.T("foo"), builder.On(builder.C("a").IsNull())),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				JoinsAppend(
					exp.NewConditionedJoinExpression(exp.LeftJoinType, builder.T("foo"), builder.On(builder.C("a").IsNull())),
				),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestNaturalJoin() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.NaturalJoin(builder.T("foo")),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				JoinsAppend(
					exp.NewUnConditionedJoinExpression(exp.NaturalJoinType, builder.T("foo")),
				),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestNaturalLeftJoin() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.NaturalLeftJoin(builder.T("foo")),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				JoinsAppend(
					exp.NewUnConditionedJoinExpression(exp.NaturalLeftJoinType, builder.T("foo")),
				),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestNaturalRightJoin() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.NaturalRightJoin(builder.T("foo")),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				JoinsAppend(
					exp.NewUnConditionedJoinExpression(exp.NaturalRightJoinType, builder.T("foo")),
				),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestNaturalFullJoin() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.NaturalFullJoin(builder.T("foo")),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				JoinsAppend(
					exp.NewUnConditionedJoinExpression(exp.NaturalFullJoinType, builder.T("foo")),
				),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestCrossJoin() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.CrossJoin(builder.T("foo")),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				JoinsAppend(
					exp.NewUnConditionedJoinExpression(exp.CrossJoinType, builder.T("foo")),
				),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestWhere() {
	w := builder.Ex{"a": 1}
	w2 := builder.Ex{"b": "c"}
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.Where(w),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				WhereAppend(w),
		},
		selectTestCase{
			ds: bd.Where(w).Where(w2),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				WhereAppend(w).WhereAppend(w2),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestClearWhere() {
	w := builder.Ex{"a": 1}
	bd := builder.From("test").Where(w)
	sds.assertCases(
		selectTestCase{
			ds: bd.ClearWhere(),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")).WhereAppend(w),
		},
	)
}

func (sds *selectDatasetSuite) TestForUpdate() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.ForUpdate(builder.NoWait),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLock(exp.NewLock(exp.ForUpdate, builder.NoWait)),
		},
		selectTestCase{
			ds: bd.ForUpdate(builder.NoWait, builder.T("table1")),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLock(exp.NewLock(exp.ForUpdate, builder.NoWait, builder.T("table1"))),
		},
		selectTestCase{
			ds: bd.ForUpdate(builder.NoWait, builder.T("table1"), builder.T("table2")),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLock(exp.NewLock(exp.ForUpdate, builder.NoWait, builder.T("table1"), builder.T("table2"))),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestForNoKeyUpdate() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.ForNoKeyUpdate(builder.NoWait),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLock(exp.NewLock(exp.ForNoKeyUpdate, builder.NoWait)),
		},
		selectTestCase{
			ds: bd.ForNoKeyUpdate(builder.NoWait, builder.T("table1")),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLock(exp.NewLock(exp.ForNoKeyUpdate, builder.NoWait, builder.T("table1"))),
		},
		selectTestCase{
			ds: bd.ForNoKeyUpdate(builder.NoWait, builder.T("table1"), builder.T("table2")),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLock(exp.NewLock(exp.ForNoKeyUpdate, builder.NoWait, builder.T("table1"), builder.T("table2"))),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestForKeyShare() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.ForKeyShare(builder.NoWait),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLock(exp.NewLock(exp.ForKeyShare, builder.NoWait)),
		},
		selectTestCase{
			ds: bd.ForKeyShare(builder.NoWait, builder.T("table1")),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLock(exp.NewLock(exp.ForKeyShare, builder.NoWait, builder.T("table1"))),
		},
		selectTestCase{
			ds: bd.ForKeyShare(builder.NoWait, builder.T("table1"), builder.T("table2")),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLock(exp.NewLock(exp.ForKeyShare, builder.NoWait, builder.T("table1"), builder.T("table2"))),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestForShare() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.ForShare(builder.NoWait),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLock(exp.NewLock(exp.ForShare, builder.NoWait)),
		},
		selectTestCase{
			ds: bd.ForShare(builder.NoWait, builder.T("table1")),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLock(exp.NewLock(exp.ForShare, builder.NoWait, builder.T("table1"))),
		},
		selectTestCase{
			ds: bd.ForShare(builder.NoWait, builder.T("table1"), builder.T("table2")),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLock(exp.NewLock(exp.ForShare, builder.NoWait, builder.T("table1"), builder.T("table2"))),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestGroupBy() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.GroupBy("a"),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetGroupBy(exp.NewColumnListExpression("a")),
		},
		selectTestCase{
			ds: bd.GroupBy("a").GroupBy("b"),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetGroupBy(exp.NewColumnListExpression("b")),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestWindow() {
	w1 := builder.W("w1").PartitionBy("a").OrderBy("b")
	w2 := builder.W("w2").PartitionBy("a").OrderBy("b")

	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.Window(w1),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				WindowsAppend(w1),
		},
		selectTestCase{
			ds: bd.Window(w1).Window(w2),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				WindowsAppend(w2),
		},
		selectTestCase{
			ds: bd.Window(w1, w2),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				WindowsAppend(w1, w2),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestWindowAppend() {
	w1 := builder.W("w1").PartitionBy("a").OrderBy("b")
	w2 := builder.W("w2").PartitionBy("a").OrderBy("b")

	bd := builder.From("test").Window(w1)
	sds.assertCases(
		selectTestCase{
			ds: bd.WindowAppend(w2),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				WindowsAppend(w1, w2),
		},
		selectTestCase{
			ds: bd,
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				WindowsAppend(w1),
		},
	)
}

func (sds *selectDatasetSuite) TestClearWindow() {
	w1 := builder.W("w1").PartitionBy("a").OrderBy("b")

	bd := builder.From("test").Window(w1)
	sds.assertCases(
		selectTestCase{
			ds:      bd.ClearWindow(),
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
		selectTestCase{
			ds: bd,
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				WindowsAppend(w1),
		},
	)
}

func (sds *selectDatasetSuite) TestHaving() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.Having(builder.C("a").Gt(1)),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				HavingAppend(builder.C("a").Gt(1)),
		},
		selectTestCase{
			ds: bd.Having(builder.C("a").Gt(1)).Having(builder.Ex{"b": "c"}),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				HavingAppend(builder.C("a").Gt(1)).HavingAppend(builder.Ex{"b": "c"}),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestOrder() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.Order(builder.C("a").Asc()),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetOrder(builder.C("a").Asc()),
		},
		selectTestCase{
			ds: bd.Order(builder.C("a").Asc()).Order(builder.C("b").Asc()),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetOrder(builder.C("b").Asc()),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestOrderAppend() {
	bd := builder.From("test").Order(builder.C("a").Asc())
	sds.assertCases(
		selectTestCase{
			ds: bd.OrderAppend(builder.C("b").Asc()),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetOrder(builder.C("a").Asc(), builder.C("b").Asc()),
		},
		selectTestCase{
			ds: bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")).
				SetOrder(builder.C("a").Asc()),
		},
	)
}

func (sds *selectDatasetSuite) TestOrderPrepend() {
	bd := builder.From("test").Order(builder.C("a").Asc())
	sds.assertCases(
		selectTestCase{
			ds: bd.OrderPrepend(builder.C("b").Asc()),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetOrder(builder.C("b").Asc(), builder.C("a").Asc()),
		},
		selectTestCase{
			ds: bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")).
				SetOrder(builder.C("a").Asc()),
		},
	)
}

func (sds *selectDatasetSuite) TestClearOrder() {
	bd := builder.From("test").Order(builder.C("a").Asc())
	sds.assertCases(
		selectTestCase{
			ds: bd.ClearOrder(),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")),
		},
		selectTestCase{
			ds: bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")).
				SetOrder(builder.C("a").Asc()),
		},
	)
}

func (sds *selectDatasetSuite) TestLimit() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.Limit(10),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLimit(uint(10)),
		},
		selectTestCase{
			ds: bd.Limit(0),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")),
		},
		selectTestCase{
			ds: bd.Limit(10).Limit(2),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLimit(uint(2)),
		},
		selectTestCase{
			ds: bd.Limit(10).Limit(0),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestLimitAll() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.LimitAll(),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLimit(builder.L("ALL")),
		},
		selectTestCase{
			ds: bd.Limit(10).LimitAll(),
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLimit(builder.L("ALL")),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestClearLimit() {
	bd := builder.From("test").Limit(10)
	sds.assertCases(
		selectTestCase{
			ds:      bd.ClearLimit(),
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
		selectTestCase{
			ds: bd,
			clauses: exp.NewSelectClauses().
				SetFrom(exp.NewColumnListExpression("test")).
				SetLimit(uint(10)),
		},
	)
}

func (sds *selectDatasetSuite) TestOffset() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds:      bd.Offset(10),
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")).SetOffset(10),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestClearOffset() {
	bd := builder.From("test").Offset(10)
	sds.assertCases(
		selectTestCase{
			ds:      bd.ClearOffset(),
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")).SetOffset(10),
		},
	)
}

func (sds *selectDatasetSuite) TestUnion() {
	uds := builder.From("union_test")
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.Union(uds),
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")).
				CompoundsAppend(exp.NewCompoundExpression(exp.UnionCompoundType, uds)),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestUnionAll() {
	uds := builder.From("union_test")
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.UnionAll(uds),
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")).
				CompoundsAppend(exp.NewCompoundExpression(exp.UnionAllCompoundType, uds)),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestIntersect() {
	uds := builder.From("union_test")
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.Intersect(uds),
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")).
				CompoundsAppend(exp.NewCompoundExpression(exp.IntersectCompoundType, uds)),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestIntersectAll() {
	uds := builder.From("union_test")
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.IntersectAll(uds),
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")).
				CompoundsAppend(exp.NewCompoundExpression(exp.IntersectAllCompoundType, uds)),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestAs() {
	bd := builder.From("test")
	sds.assertCases(
		selectTestCase{
			ds: bd.As("t"),
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")).
				SetAlias(builder.T("t")),
		},
		selectTestCase{
			ds:      bd,
			clauses: exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")),
		},
	)
}

func (sds *selectDatasetSuite) TestToSQL() {
	md := new(mocks.SQLDialect)
	ds := builder.From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToSelectSQL", sqlB, c).Return(nil).Once()
	sql, args, err := ds.ToSQL()
	sds.Empty(sql)
	sds.Empty(args)
	sds.Nil(err)
	md.AssertExpectations(sds.T())
}

func (sds *selectDatasetSuite) TestToSQL_prepared() {
	md := new(mocks.SQLDialect)
	ds := builder.From("test").Prepared(true).SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(true)
	md.On("ToSelectSQL", sqlB, c).Return(nil).Once()
	sql, args, err := ds.ToSQL()
	sds.Empty(sql)
	sds.Empty(args)
	sds.Nil(err)
	md.AssertExpectations(sds.T())
}

func (sds *selectDatasetSuite) TestToSQL_ReturnedError() {
	md := new(mocks.SQLDialect)
	ds := builder.From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	ee := errors.New("expected error")
	md.On("ToSelectSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	sql, args, err := ds.ToSQL()
	sds.Empty(sql)
	sds.Empty(args)
	sds.Equal(ee, err)
	md.AssertExpectations(sds.T())
}

func (sds *selectDatasetSuite) TestAppendSQL() {
	md := new(mocks.SQLDialect)
	ds := builder.From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToSelectSQL", sqlB, c).Return(nil).Once()
	ds.AppendSQL(sqlB)
	sds.NoError(sqlB.Error())
	md.AssertExpectations(sds.T())
}

func (sds *selectDatasetSuite) TestQueryRow() {
	mDB, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	GenMock(5, func() {
		sqlMock.ExpectQuery(`SELECT "address", "name" FROM "items" LIMIT 1`).
			WithArgs().
			WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))
	})

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	bs := db.From("items").Select("address", "name")

	var item dsTestActionItem
	err = bs.QueryRow(&item)
	sds.NoError(err)
	sds.Equal("111 Test Addr", item.Address)
	sds.Equal("Test1", item.Name)
	item = dsTestActionItem{}
	err = bs.QueryRow(&item)
	sds.NoError(err)
	sds.Equal("111 Test Addr", item.Address)
	sds.Equal("Test1", item.Name)
	err = bs.QueryRow(item)
	sds.EqualError(err, InvalidPointer(item))
	err = bs.QueryRow([]dsTestActionItem{})
	sds.EqualError(err, InvalidPointer([]dsTestActionItem{}))

	err = builder.From("items").QueryRow(item)
	sds.Equal(builder.ErrExecutorNotFoundError, err)
}

func (sds *selectDatasetSuite) TestQueryRow_WithPreparedStatements() {
	mDB, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	GenMock(5, func() {
		sqlMock.ExpectQuery(
			`SELECT "address", "name" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\) LIMIT \?`,
		).
			WithArgs("111 Test Addr", "Bob", "Sally", "Billy", 1).
			WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))
	})

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	bs := db.From("items").Select("address", "name").Prepared(true).Where(builder.Ex{
		"name":    []string{"Bob", "Sally", "Billy"},
		"address": "111 Test Addr",
	})

	var item dsTestActionItem
	err = bs.QueryRow(&item)
	sds.NoError(err)
	sds.Equal("111 Test Addr", item.Address)
	sds.Equal("Test1", item.Name)

	err = bs.QueryRow(item)
	sds.EqualError(err, InvalidPointer(item))
	err = bs.QueryRow([]dsTestActionItem{})
	sds.EqualError(err, InvalidPointer([]dsTestActionItem{}))
}

func (sds *selectDatasetSuite) TestQuryRowPartial() {
	mDB, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	GenMock(3, func() {
		sqlMock.ExpectQuery(`SELECT "id" FROM "items" LIMIT 1`).
			WithArgs().
			WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	})

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	bs := db.From("items").Select("id")
	var id int64
	err = bs.QueryRowPartial(&id)
	sds.NoError(err)
	sds.Equal(id, int64(10))

	err = bs.QueryRowPartial([]int64{})
	sds.EqualError(err, InvalidPointer([]int64{}))
	err = bs.QueryRowPartial(10)
	sds.EqualError(err, InvalidPointer(10))

	err = builder.From("items").QueryRowPartial(&id)
	sds.Equal(builder.ErrExecutorNotFoundError, err)
}

func (sds *selectDatasetSuite) TestQueryRow_WithPreparedStatement() {
	mDB, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	GenMock(3, func() {
		sqlMock.ExpectQuery(
			`SELECT "id" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\) LIMIT ?`,
		).
			WithArgs("111 Test Addr", "Bob", "Sally", "Billy", 1).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))
	})

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	bs := db.From("items").Prepared(true).Select("id").Limit(1).
		Where(builder.Ex{
			"name":    []string{"Bob", "Sally", "Billy"},
			"address": "111 Test Addr",
		})

	var id int64
	err = bs.QueryRow(&id)
	sds.NoError(err)
	sds.Equal(int64(10), id)

	err = bs.QueryRow([]int64{})
	sds.EqualError(err, InvalidPointer([]int64{}))
	err = bs.QueryRow(10)
	sds.EqualError(err, InvalidPointer(10))
}

func (sds *selectDatasetSuite) TestQueryRows() {
	mDB, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	GenMock(4, func() {
		sqlMock.ExpectQuery(`SELECT "address", "name" FROM "items"`).
			WithArgs().
			WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
				FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))
	})

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	bs := db.From("items").Select("address", "name")
	var items []dsTestActionItem
	sds.NoError(bs.QueryRows(&items))
	sds.Equal([]dsTestActionItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	}, items)

	items = items[0:0]
	sds.NoError(bs.QueryRows(&items))
	sds.Equal([]dsTestActionItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	}, items)

	items = items[0:0]
	sds.EqualError(bs.QueryRows(items),
		InvalidPointer(items))
	sds.EqualError(bs.QueryRows(&dsTestActionItem{}),
		UnsupportedType())

	sds.Equal(builder.ErrExecutorNotFoundError, builder.From("items").QueryRows(items))
}

func (sds *selectDatasetSuite) TestQueryRows_WithPreparedStatements() {
	mDB, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	GenMock(4, func() {
		sqlMock.ExpectQuery(
			`SELECT "address", "name" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
		).
			WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
			WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
				FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))
	})

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	bs := db.From("items").Select("address", "name").Prepared(true).
		Where(builder.Ex{
			"name":    []string{"Bob", "Sally", "Billy"},
			"address": "111 Test Addr",
		})
	var items []dsTestActionItem
	sds.NoError(bs.QueryRows(&items))
	sds.Equal(items, []dsTestActionItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	})

	items = items[0:0]
	sds.EqualError(bs.QueryRows(items),
		InvalidPointer(items))
	sds.EqualError(bs.QueryRows(&dsTestActionItem{}),
		UnsupportedType())
}

func (sds *selectDatasetSuite) TestQueryRowsPartial() {
	mDB, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	GenMock(4, func() {
		sqlMock.ExpectQuery(`SELECT "id" FROM "items"`).
			WithArgs().
			WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))
	})

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	bs := db.From("items").Select("id")

	var ids []uint32
	sds.NoError(bs.QueryRowsPartial(&ids))
	sds.Equal(ids, []uint32{1, 2, 3, 4, 5})

	sds.EqualError(bs.QueryRowPartial([]uint32{}),
		InvalidPointer([]uint32{}))
	sds.EqualError(bs.QueryRowsPartial(dsTestActionItem{}),
		InvalidPointer(dsTestActionItem{}))

	err = builder.From("items").QueryRowsPartial(&ids)
	sds.Equal(builder.ErrExecutorNotFoundError, err)
}

func (sds *selectDatasetSuite) TestQueryRowsPartial_WithPreparedStatment() {
	mDB, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	GenMock(3, func() {
		sqlMock.ExpectQuery(
			`SELECT "id" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
		).
			WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
			WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))

	})

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	bs := db.From("items").Prepared(true).Select("id").
		Where(builder.Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"})

	var ids []uint32
	sds.NoError(bs.QueryRows(&ids))
	sds.Equal([]uint32{1, 2, 3, 4, 5}, ids)

	sds.EqualError(bs.QueryRows([]uint32{}),
		InvalidPointer([]uint32{}))
	sds.EqualError(bs.QueryRows(dsTestActionItem{}),
		InvalidPointer(dsTestActionItem{}))
}

func (sds *selectDatasetSuite) TestCount() {
	mDB, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	sqlMock.ExpectQuery(`SELECT COUNT\(\*\) AS "count" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"count"}).FromCSVString("10"))

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	count, err := db.From("items").Count()
	sds.NoError(err)
	sds.Equal(count, int64(10))
}

func (sds *selectDatasetSuite) TestCount_WithPreparedStatement() {
	mDB, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	sqlMock.ExpectQuery(
		`SELECT COUNT\(\*\) AS "count" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).FromCSVString("10"))

	conn := sqlx.NewSqlConnFromDB(mDB)
	ds := builder.New("mock", conn)
	bs := ds.From("items").Prepared(true).
		Where(builder.Ex{
			"name":    []string{"Bob", "Sally", "Billy"},
			"address": "111 Test Addr",
		})
	count, err := bs.Count()
	sds.NoError(err)
	sds.Equal(int64(10), count)
}

func (sds *selectDatasetSuite) TestPluck() {
	mDB, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	sqlMock.ExpectQuery(`SELECT "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"name"}).FromCSVString("test1\ntest2\ntest3\ntest4\ntest5"))

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	var names []string
	sds.NoError(db.From("items").Pluck(&names, "name"))
	sds.Equal([]string{"test1", "test2", "test3", "test4", "test5"}, names)
}

func (sds *selectDatasetSuite) TestPluck_WithPreparedStatement() {
	mDB, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	sqlMock.ExpectQuery(
		`SELECT "name" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnRows(sqlmock.NewRows([]string{"name"}).FromCSVString("Bob\nSally\nBilly"))

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	var names []string
	sds.NoError(db.From("items").
		Prepared(true).
		Where(builder.Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		Pluck(&names, "name"))
	sds.Equal([]string{"Bob", "Sally", "Billy"}, names)
}

func (sds *selectDatasetSuite) TestSetError() {
	err1 := errors.New("error #1")
	err2 := errors.New("error #2")
	err3 := errors.New("error #3")

	// Verify initial error set/get works properly
	md := new(mocks.SQLDialect)
	ds := builder.From("test").SetDialect(md)
	ds = ds.SetError(err1)
	sds.Equal(err1, ds.Error())
	sql, args, err := ds.ToSQL()
	sds.Empty(sql)
	sds.Empty(args)
	sds.Equal(err1, err)

	// Repeated SetError calls on Dataset should not overwrite the original error
	ds = ds.SetError(err2)
	sds.Equal(err1, ds.Error())
	sql, args, err = ds.ToSQL()
	sds.Empty(sql)
	sds.Empty(args)
	sds.Equal(err1, err)

	// Builder functions should not lose the error
	ds = ds.ClearWindow()
	sds.Equal(err1, ds.Error())
	sql, args, err = ds.ToSQL()
	sds.Empty(sql)
	sds.Empty(args)
	sds.Equal(err1, err)

	// Deeper errors inside SQL generation should still return original error
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToInsertSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(err3)
	}).Once()

	sql, args, err = ds.ToSQL()
	sds.Empty(sql)
	sds.Empty(args)
	sds.Equal(err1, err)
}

func TestSelectDataset(t *testing.T) {
	suite.Run(t, new(selectDatasetSuite))
}
