package exp_test

import (
	"testing"

	"github.com/Tooooommy/builder/v9/exp"
	"github.com/stretchr/testify/suite"
)

type lateralExpressionSuite struct {
	suite.Suite
}

func TestLateralExpressionSuite(t *testing.T) {
	suite.Run(t, &lateralExpressionSuite{})
}

func (les *lateralExpressionSuite) TestClone() {
	le := exp.NewLateralExpression(newTestAppendableExpression(`SELECT * FROM "test"`, []any{}))
	les.Equal(exp.NewLateralExpression(newTestAppendableExpression(`SELECT * FROM "test"`, []any{})), le.Clone())
}

func (les *lateralExpressionSuite) TestExpression() {
	le := exp.NewLateralExpression(newTestAppendableExpression(`SELECT * FROM "test"`, []any{}))
	les.Equal(le, le.Expression())
}

func (les *lateralExpressionSuite) TestLateral() {
	le := exp.NewLateralExpression(newTestAppendableExpression(`SELECT * FROM "test"`, []any{}))
	les.Equal(newTestAppendableExpression(`SELECT * FROM "test"`, []any{}), le.Table())
}

func (les *lateralExpressionSuite) TestAs() {
	le := exp.NewLateralExpression(newTestAppendableExpression(`SELECT * FROM "test"`, []any{}))
	les.Equal(exp.NewAliasExpression(le, "foo"), le.As("foo"))
}
