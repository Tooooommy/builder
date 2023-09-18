package builder_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Tooooommy/builder/v9"
	"github.com/Tooooommy/builder/v9/internal/errors"
	"github.com/stretchr/testify/suite"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type testItem struct {
	Address string `db:"address"`
	Name    string `db:"name"`
}

func InvalidPointer(v any) string {
	return fmt.Sprintf("not a valid pointer: %v", v)
}

func UnsupportedType() string {
	return "unsupported unmarshal type"
}

func NotMatchDestination() string {
	return sqlx.ErrNotMatchDestination.Error()
}

func TransactFailed() string {
	return "transaction failed: builder: something wrong, rollback failed: builder: transaction rollback error"
}

type databaseSuite struct {
	suite.Suite
}

func (ds *databaseSuite) TestExec() {
	mDB, mock, err := sqlmock.New()
	ds.NoError(err)

	mock.ExpectExec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE \("name" IS NULL\)`).
		WithArgs().
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectExec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE \("name" IS NULL\)`).
		WithArgs().
		WillReturnError(errors.New("mock error"))

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)

	_, err = db.Exec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`)
	ds.NoError(err)
	_, err = db.Exec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`)
	ds.EqualError(err, "builder: mock error")
}

func (ds *databaseSuite) TestPrepare() {
	mDB, mock, err := sqlmock.New()
	ds.NoError(err)
	mock.ExpectPrepare("SELECT \\* FROM test WHERE id = \\?")

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)

	stmt, err := db.Prepare("SELECT * FROM test WHERE id = ?")
	ds.NoError(err)
	ds.NotNil(stmt)
}

func (ds *databaseSuite) TestQueryRow() {
	mDB, mock, err := sqlmock.New()
	ds.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(
			sqlmock.NewRows([]string{"address", "name"}).
				AddRow("111 Test Addr", "Test1"))

	mock.ExpectQuery(`SELECT "test" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(
			sqlmock.NewRows([]string{"test"}).
				AddRow("test1\ntest2"))

	mock.ExpectQuery(`SELECT \* FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(
			sqlmock.NewRows([]string{"address", "name"}).
				AddRow("111 Test Addr", "Test1"))

	mock.ExpectQuery(`SELECT "test" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(
			sqlmock.NewRows([]string{"test"}).
				AddRow("test1\ntest2"))

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)

	var item testItem
	err = db.QueryRow(&item, `SELECT * FROM "items" LIMIT 1`)
	ds.NoError(err)
	ds.Equal("111 Test Addr", item.Address)
	ds.Equal("Test1", item.Name)
	err = db.QueryRow(item, `SELECT "test" FROM "items" LIMIT 1`)
	ds.EqualError(err, InvalidPointer(item))
	err = db.QueryRow([]testItem{}, `SELECT * FROM "items" LIMIT 1`)
	ds.EqualError(err, InvalidPointer([]testItem{}))
	err = db.QueryRow(&item, `SELECT "test" FROM "items" LIMIT 1`)
	ds.EqualError(err, NotMatchDestination())
}

func (ds *databaseSuite) TestQueryRowPartial() {
	mDB, mock, err := sqlmock.New()
	ds.NoError(err)

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	var id int64
	err = db.QueryRowPartial(&id, `SELECT "id" FROM "items"`)
	ds.NoError(err)
	ds.Equal(int64(10), id)

	err = db.QueryRowPartial([]int64{}, `SELECT "id" FROM "items"`)
	ds.EqualError(err, InvalidPointer([]int64{}))
	err = db.QueryRowPartial(10, `SELECT "id" FROM "items"`)
	ds.EqualError(err, InvalidPointer(10))
}

func (ds *databaseSuite) TestQueryRows() {
	mDB, mock, err := sqlmock.New()
	ds.NoError(err)
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))
	mock.ExpectQuery(`SELECT "test" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("db-mock", conn)

	var items []testItem
	ds.NoError(db.QueryRows(&items, `SELECT * FROM "items"`))
	ds.Len(items, 2)
	ds.Equal("111 Test Addr", items[0].Address)
	ds.Equal("Test1", items[0].Name)

	ds.Equal("211 Test Addr", items[1].Address)
	ds.Equal("Test2", items[1].Name)

	items = items[0:0]
	ds.EqualError(db.QueryRows(items, `SELECT * FROM "items"`),
		InvalidPointer(items))
	ds.EqualError(db.QueryRows(&testItem{}, `SELECT * FROM "items"`),
		UnsupportedType())
	ds.EqualError(db.QueryRows(&items, `SELECT "test" FROM "items"`),
		NotMatchDestination())
}

func (ds *databaseSuite) TestQueryRowsPartial() {
	mDB, mock, err := sqlmock.New()
	ds.NoError(err)
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	var ids []uint32

	ds.NoError(db.QueryRowsPartial(&ids, `SELECT "id" FROM "items"`))
	ds.Len(ids, 5)
	ds.EqualError(db.QueryRowsPartial([]uint32{}, `SELECT "id" FROM "items"`),
		InvalidPointer([]uint32{}))
	ds.EqualError(db.QueryRowsPartial(testItem{}, `SELECT "id" FROM "items"`),
		InvalidPointer(testItem{}))
}

func (ds *databaseSuite) TestTransact() {
	mDB, mock, err := sqlmock.New()
	ds.NoError(err)

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)

	cases := []struct {
		expectf func(sqlmock.Sqlmock)
		f       func(*builder.TxDatabase) error
		wantErr bool
		errStr  string
	}{
		{
			expectf: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectCommit()
			},
			f:       func(_ *builder.TxDatabase) error { return nil },
			wantErr: false,
		},
		{
			expectf: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin().WillReturnError(errors.New("transaction begin error"))
			},
			f:       func(_ *builder.TxDatabase) error { return nil },
			wantErr: true,
			errStr:  "builder: transaction begin error",
		},
		{
			expectf: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectRollback()
			},
			f:       func(_ *builder.TxDatabase) error { return errors.New("transaction error") },
			wantErr: true,
			errStr:  "builder: transaction error",
		},
		{
			expectf: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectRollback().WillReturnError(errors.New("transaction rollback error"))
			},
			f:       func(_ *builder.TxDatabase) error { return errors.New("something wrong") },
			wantErr: true,
			errStr:  TransactFailed(),
		},
		{
			expectf: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectCommit().WillReturnError(errors.New("commit error"))
			},
			f:       func(_ *builder.TxDatabase) error { return nil },
			wantErr: true,
			errStr:  "builder: commit error",
		},
	}
	for _, c := range cases {
		c.expectf(mock)
		err := db.Transact(c.f)
		if c.wantErr {
			ds.EqualError(err, c.errStr)
		} else {
			ds.NoError(err)
		}
	}
}

func (ds *databaseSuite) TestDataRace() {
	mDB, mock, err := sqlmock.New()
	ds.NoError(err)

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)

	const concurrency = 10

	for i := 0; i < concurrency; i++ {
		mock.ExpectQuery(`SELECT \* FROM "items"`).
			WithArgs().
			WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
				FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))
	}

	wg := sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			sql := db.From("items").Limit(1)
			var item testItem
			err := sql.QueryRow(&item)
			ds.NoError(err)
			ds.Equal(item.Address, "111 Test Addr")
			ds.Equal(item.Name, "Test1")
		}()
	}

	wg.Wait()
}

func TestDatabaseSuite(t *testing.T) {
	suite.Run(t, new(databaseSuite))
}

type txdatabaseSuite struct {
	suite.Suite
}

func (tds *txdatabaseSuite) TestFrom() {
	mDB, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectCommit()

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	db.Transact(func(td *builder.TxDatabase) error {
		tds.NotNil(builder.From("test"))
		return nil
	})
}

func (tds *txdatabaseSuite) TestTxExec() {
	mDB, mock, err := sqlmock.New()
	tds.NoError(err)

	mock.ExpectBegin()

	mock.ExpectExec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE \("name" IS NULL\)`).
		WithArgs().
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectExec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE \("name" IS NULL\)`).
		WithArgs().
		WillReturnError(errors.New("mock error"))

	mock.ExpectCommit()

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)

	db.Transact(func(td *builder.TxDatabase) error {
		_, err = td.Exec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`)
		tds.NoError(err)
		_, err = td.Exec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`)
		tds.EqualError(err, "builder: mock error")
		return err
	})
}

func (tds *txdatabaseSuite) TestTxQueryRow() {
	mDB, mock, err := sqlmock.New()
	tds.NoError(err)

	mock.ExpectBegin()

	mock.ExpectQuery(`SELECT \* FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	mock.ExpectQuery(`SELECT \* FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	mock.ExpectQuery(`SELECT \* FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	mock.ExpectQuery(`SELECT "test" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	mock.ExpectCommit()

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	var item testItem
	db.Transact(func(td *builder.TxDatabase) error {
		err := td.QueryRow(&item, `SELECT * FROM "items" LIMIT 1`)
		tds.NoError(err)
		tds.Equal("111 Test Addr", item.Address)
		tds.Equal("Test1", item.Name)

		err = td.QueryRow(item, `SELECT * FROM "items" LIMIT 1`)
		tds.EqualError(err, InvalidPointer(item))
		err = td.QueryRow([]testItem{}, `SELECT * FROM "items" LIMIT 1`)
		tds.EqualError(err, InvalidPointer([]testItem{}))
		err = td.QueryRow(&item, `SELECT "test" FROM "items" LIMIT 1`)
		tds.EqualError(err, NotMatchDestination())
		return err
	})
}

func (tds *txdatabaseSuite) TestTxQueryRowPartial() {
	mDB, mock, err := sqlmock.New()
	tds.NoError(err)

	mock.ExpectBegin()

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	mock.ExpectCommit()

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	var id int64
	db.Transact(func(td *builder.TxDatabase) error {
		err := td.QueryRowPartial(&id, `SELECT "id" FROM "items"`)
		tds.NoError(err)
		tds.Equal(int64(10), id)
		err = td.QueryRowPartial([]int64{}, `SELECT "id" FROM "items"`)
		tds.EqualError(err, InvalidPointer([]int64{}))
		err = td.QueryRowPartial(10, `SELECT "id" FROM "items"`)
		tds.EqualError(err, InvalidPointer(10))
		return err
	})
}

func (tds *txdatabaseSuite) TestTxQueryRows() {
	mDB, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))
	mock.ExpectQuery(`SELECT "test" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))
	mock.ExpectCommit()

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	var items []testItem
	db.Transact(func(td *builder.TxDatabase) error {
		tds.NoError(td.QueryRows(&items, `SELECT * FROM "items"`))
		tds.Len(items, 2)
		tds.Equal("111 Test Addr", items[0].Address)
		tds.Equal("Test1", items[0].Name)
		tds.Equal("211 Test Addr", items[1].Address)
		tds.Equal("Test2", items[1].Name)
		items = items[0:0]
		tds.EqualError(td.QueryRows(items, `SELECT * FROM "items"`),
			InvalidPointer(items))
		tds.EqualError(td.QueryRows(&testItem{}, `SELECT * FROM "items"`),
			UnsupportedType())
		tds.EqualError(td.QueryRows(&items, `SELECT "test" FROM "items"`),
			NotMatchDestination())
		return err
	})
}

func (tds *txdatabaseSuite) TestTxQueryRowsPartial() {
	mDB, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))
	mock.ExpectCommit()

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	var ids []uint32
	db.Transact(func(td *builder.TxDatabase) error {
		tds.NoError(td.QueryRowsPartial(&ids, `SELECT "id" FROM "items"`))
		tds.Len(ids, 5)
		tds.EqualError(td.QueryRowsPartial([]uint32{}, `SELECT "id" FROM "items"`),
			InvalidPointer([]uint32{}))
		tds.EqualError(td.QueryRowsPartial(testItem{}, `SELECT "id" FROM "items"`),
			InvalidPointer(testItem{}))
		return err
	})
}

func (tds *txdatabaseSuite) TestTxDataRace() {
	mDB, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()

	conn := sqlx.NewSqlConnFromDB(mDB)
	db := builder.New("mock", conn)
	db.Transact(func(td *builder.TxDatabase) error {
		const concurrency = 10

		for i := 0; i < concurrency; i++ {
			mock.ExpectQuery(`SELECT "address", "name" FROM "items"`).
				WithArgs().
				WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
					FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))
		}

		wg := sync.WaitGroup{}
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				sql := td.Select("address", "name").From("items").Limit(1)
				var item testItem
				err := sql.QueryRow(&item)
				tds.NoError(err)
				tds.Equal(item.Address, "111 Test Addr")
				tds.Equal(item.Name, "Test1")
			}()
		}

		wg.Wait()
		mock.ExpectCommit()
		return nil
	})
}

func TestTxDatabaseSuite(t *testing.T) {
	suite.Run(t, new(txdatabaseSuite))
}
