package builder_test

import (
	"testing"

	"github.com/Tooooommy/builder/v9"
	"github.com/Tooooommy/builder/v9/exp"
	"github.com/stretchr/testify/suite"
)

type (
	builderExpressionsSuite struct {
		suite.Suite
	}
)

func (ges *builderExpressionsSuite) TestCast() {
	ges.Equal(exp.NewCastExpression(builder.C("test"), "string"), builder.Cast(builder.C("test"), "string"))
}

func (ges *builderExpressionsSuite) TestDoNothing() {
	ges.Equal(exp.NewDoNothingConflictExpression(), builder.DoNothing())
}

func (ges *builderExpressionsSuite) TestDoUpdate() {
	ges.Equal(exp.NewDoUpdateConflictExpression("test", builder.Record{"a": "b"}), builder.DoUpdate("test", builder.Record{"a": "b"}))
}

func (ges *builderExpressionsSuite) TestOr() {
	e1 := builder.C("a").Eq("b")
	e2 := builder.C("b").Eq(2)
	ges.Equal(exp.NewExpressionList(exp.OrType, e1, e2), builder.Or(e1, e2))
}

func (ges *builderExpressionsSuite) TestAnd() {
	e1 := builder.C("a").Eq("b")
	e2 := builder.C("b").Eq(2)
	ges.Equal(exp.NewExpressionList(exp.AndType, e1, e2), builder.And(e1, e2))
}

func (ges *builderExpressionsSuite) TestFunc() {
	ges.Equal(exp.NewSQLFunctionExpression("count", builder.L("*")), builder.Func("count", builder.L("*")))
}

func (ges *builderExpressionsSuite) TestDISTINCT() {
	ges.Equal(exp.NewSQLFunctionExpression("DISTINCT", builder.I("col")), builder.DISTINCT("col"))
}

func (ges *builderExpressionsSuite) TestCOUNT() {
	ges.Equal(exp.NewSQLFunctionExpression("COUNT", builder.I("col")), builder.COUNT("col"))
}

func (ges *builderExpressionsSuite) TestMIN() {
	ges.Equal(exp.NewSQLFunctionExpression("MIN", builder.I("col")), builder.MIN("col"))
}

func (ges *builderExpressionsSuite) TestMAX() {
	ges.Equal(exp.NewSQLFunctionExpression("MAX", builder.I("col")), builder.MAX("col"))
}

func (ges *builderExpressionsSuite) TestAVG() {
	ges.Equal(exp.NewSQLFunctionExpression("AVG", builder.I("col")), builder.AVG("col"))
}

func (ges *builderExpressionsSuite) TestFIRST() {
	ges.Equal(exp.NewSQLFunctionExpression("FIRST", builder.I("col")), builder.FIRST("col"))
}

func (ges *builderExpressionsSuite) TestLAST() {
	ges.Equal(exp.NewSQLFunctionExpression("LAST", builder.I("col")), builder.LAST("col"))
}

func (ges *builderExpressionsSuite) TestSUM() {
	ges.Equal(exp.NewSQLFunctionExpression("SUM", builder.I("col")), builder.SUM("col"))
}

func (ges *builderExpressionsSuite) TestCOALESCE() {
	ges.Equal(exp.NewSQLFunctionExpression("COALESCE", builder.I("col"), nil), builder.COALESCE(builder.I("col"), nil))
}

func (ges *builderExpressionsSuite) TestROW_NUMBER() {
	ges.Equal(exp.NewSQLFunctionExpression("ROW_NUMBER"), builder.ROW_NUMBER())
}

func (ges *builderExpressionsSuite) TestRANK() {
	ges.Equal(exp.NewSQLFunctionExpression("RANK"), builder.RANK())
}

func (ges *builderExpressionsSuite) TestDENSE_RANK() {
	ges.Equal(exp.NewSQLFunctionExpression("DENSE_RANK"), builder.DENSE_RANK())
}

func (ges *builderExpressionsSuite) TestPERCENT_RANK() {
	ges.Equal(exp.NewSQLFunctionExpression("PERCENT_RANK"), builder.PERCENT_RANK())
}

func (ges *builderExpressionsSuite) TestCUME_DIST() {
	ges.Equal(exp.NewSQLFunctionExpression("CUME_DIST"), builder.CUME_DIST())
}

func (ges *builderExpressionsSuite) TestNTILE() {
	ges.Equal(exp.NewSQLFunctionExpression("NTILE", 1), builder.NTILE(1))
}

func (ges *builderExpressionsSuite) TestFIRST_VALUE() {
	ges.Equal(exp.NewSQLFunctionExpression("FIRST_VALUE", builder.I("col")), builder.FIRST_VALUE("col"))
}

func (ges *builderExpressionsSuite) TestLAST_VALUE() {
	ges.Equal(exp.NewSQLFunctionExpression("LAST_VALUE", builder.I("col")), builder.LAST_VALUE("col"))
}

func (ges *builderExpressionsSuite) TestNTH_VALUE() {
	ges.Equal(exp.NewSQLFunctionExpression("NTH_VALUE", builder.I("col"), 1), builder.NTH_VALUE("col", 1))
	ges.Equal(exp.NewSQLFunctionExpression("NTH_VALUE", builder.I("col"), 1), builder.NTH_VALUE(builder.C("col"), 1))
}

func (ges *builderExpressionsSuite) TestI() {
	ges.Equal(exp.NewIdentifierExpression("s", "t", "c"), builder.I("s.t.c"))
}

func (ges *builderExpressionsSuite) TestC() {
	ges.Equal(exp.NewIdentifierExpression("", "", "c"), builder.C("c"))
}

func (ges *builderExpressionsSuite) TestS() {
	ges.Equal(exp.NewIdentifierExpression("s", "", ""), builder.S("s"))
}

func (ges *builderExpressionsSuite) TestT() {
	ges.Equal(exp.NewIdentifierExpression("", "t", ""), builder.T("t"))
}

func (ges *builderExpressionsSuite) TestW() {
	ges.Equal(exp.NewWindowExpression(nil, nil, nil, nil), builder.W())
	ges.Equal(exp.NewWindowExpression(builder.I("a"), nil, nil, nil), builder.W("a"))
	ges.Equal(exp.NewWindowExpression(builder.I("a"), builder.I("b"), nil, nil), builder.W("a", "b"))
	ges.Equal(exp.NewWindowExpression(builder.I("a"), builder.I("b"), nil, nil), builder.W("a", "b", "c"))
}

func (ges *builderExpressionsSuite) TestOn() {
	ges.Equal(exp.NewJoinOnCondition(builder.Ex{"a": "b"}), builder.On(builder.Ex{"a": "b"}))
}

func (ges *builderExpressionsSuite) TestUsing() {
	ges.Equal(exp.NewJoinUsingCondition("a", "b"), builder.Using("a", "b"))
}

func (ges *builderExpressionsSuite) TestL() {
	ges.Equal(exp.NewLiteralExpression("? + ?", 1, 2), builder.L("? + ?", 1, 2))
}

func (ges *builderExpressionsSuite) TestLiteral() {
	ges.Equal(exp.NewLiteralExpression("? + ?", 1, 2), builder.Literal("? + ?", 1, 2))
}

func (ges *builderExpressionsSuite) TestV() {
	ges.Equal(exp.NewLiteralExpression("?", "a"), builder.V("a"))
}

func (ges *builderExpressionsSuite) TestRange() {
	ges.Equal(exp.NewRangeVal("a", "b"), builder.Range("a", "b"))
}

func (ges *builderExpressionsSuite) TestStar() {
	ges.Equal(exp.NewLiteralExpression("*"), builder.Star())
}

func (ges *builderExpressionsSuite) TestDefault() {
	ges.Equal(exp.Default(), builder.Default())
}

func (ges *builderExpressionsSuite) TestLateral() {
	ds := builder.From("test")
	ges.Equal(exp.NewLateralExpression(ds), builder.Lateral(ds))
}

func (ges *builderExpressionsSuite) TestAny() {
	ds := builder.From("test").Select("id")
	ges.Equal(exp.NewSQLFunctionExpression("ANY ", ds), builder.Any(ds))
}

func (ges *builderExpressionsSuite) TestAll() {
	ds := builder.From("test").Select("id")
	ges.Equal(exp.NewSQLFunctionExpression("ALL ", ds), builder.All(ds))
}

func TestbuilderExpressions(t *testing.T) {
	suite.Run(t, new(builderExpressionsSuite))
}
