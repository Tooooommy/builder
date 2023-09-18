package builder_test

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Tooooommy/builder/v9"
	"github.com/stretchr/testify/suite"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type (
	dialectWrapperSuite struct {
		suite.Suite
	}
)

func (dws *dialectWrapperSuite) SetupSuite() {
	testDialect := builder.DefaultDialectOptions()
	// override to some value to ensure correct dialect is set
	builder.RegisterDialect("test", testDialect)
}

func (dws *dialectWrapperSuite) TearDownSuite() {
	builder.DeregisterDialect("test")
}

func (dws *dialectWrapperSuite) TestFrom() {
	dw := builder.Dialect("test")
	dws.Equal(builder.From("table").WithDialect("test"), dw.From("table"))
}

func (dws *dialectWrapperSuite) TestSelect() {
	dw := builder.Dialect("test")
	dws.Equal(builder.Select("col").WithDialect("test"), dw.Select("col"))
}

func (dws *dialectWrapperSuite) TestInsert() {
	dw := builder.Dialect("test")
	dws.Equal(builder.Insert("table").WithDialect("test"), dw.Insert("table"))
}

func (dws *dialectWrapperSuite) TestDelete() {
	dw := builder.Dialect("test")
	dws.Equal(builder.Delete("table").WithDialect("test"), dw.Delete("table"))
}

func (dws *dialectWrapperSuite) TestTruncate() {
	dw := builder.Dialect("test")
	dws.Equal(builder.Truncate("table").WithDialect("test"), dw.Truncate("table"))
}

func (dws *dialectWrapperSuite) TestDB() {
	mDB, _, err := sqlmock.New()
	dws.Require().NoError(err)
	conn := sqlx.NewSqlConnFromDB(mDB)
	dw := builder.Dialect("test")
	dws.Equal(builder.New("test", conn), dw.DB(conn))
}

func TestDialectWrapper(t *testing.T) {
	suite.Run(t, new(dialectWrapperSuite))
}
