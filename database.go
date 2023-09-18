package builder

import (
	"context"
	"database/sql"

	"github.com/Tooooommy/builder/v9/internal/errors"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var ErrExecutorNotFoundError = errors.New(
	"unable to execute query did you use builder.Database#From to create the dataset",
)

// This struct is the wrapper for a Db. The struct delegates most calls to either an Exec instance or to the Db
// passed into the constructor.
type Database struct {
	logger  logx.Logger
	dialect string
	// nolint: stylecheck // keep for backwards compatibility
	conn sqlx.SqlConn
}

// This is the common entry point into builder.
//
// dialect: This is the adapter dialect, you should see your database adapter for the string to use. Built in adapters
// can be found at https://github.com/Tooooommy/builder/tree/master/adapters
func newDatabase(dialect string, conn sqlx.SqlConn) *Database {

	return &Database{
		logger:  logx.WithCallerSkip(-1),
		dialect: dialect,
		conn:    conn,
	}
}

// returns this databases dialect
func (d *Database) Dialect() string {
	return d.dialect
}

// Creates a new Dataset that uses the correct adapter and supports queries.
//
//	var ids []uint32
//	if err := db.From("items").Where(builder.I("id").Gt(10)).Pluck("id", &ids); err != nil {
//	    panic(err.Error())
//	}
//	fmt.Printf("%+v", ids)
//
// from...: Sources for you dataset, could be table names (strings), a builder.Literal or another builder.Dataset
func (d *Database) From(from ...any) *SelectDataset {
	return newDataset(d.dialect, d.conn).From(from...)
}

func (d *Database) Select(cols ...any) *SelectDataset {
	return newDataset(d.dialect, d.conn).Select(cols...)
}

func (d *Database) Update(table any) *UpdateDataset {
	return newUpdateDataset(d.dialect, d.conn).Table(table)
}

func (d *Database) Insert(table any) *InsertDataset {
	return newInsertDataset(d.dialect, d.conn).Into(table)
}

func (d *Database) Delete(table any) *DeleteDataset {
	return newDeleteDataset(d.dialect, d.conn).From(table)
}

func (d *Database) Truncate(table ...any) *TruncateDataset {
	return newTruncateDataset(d.dialect, d.conn).Table(table...)
}

// Sets the logger for to use when logging queries
func (d *Database) Logger(logger logx.Logger) {
	d.logger = logger
}

// Logs a given operation with the specified sql and arguments
func (d *Database) Trace(ctx context.Context, op, sqlString string, args ...any) {
	if d.logger != nil {
		if sqlString != "" {
			if len(args) != 0 {
				d.logger.WithContext(ctx).Infof("[builder] %s [query:=`%s` args:=%+v]", op, sqlString, args)
			} else {
				d.logger.WithContext(ctx).Infof("[builder] %s [query:=`%s`]", op, sqlString)
			}
		} else {
			d.logger.WithContext(ctx).Infof("[builder] %s", op)
		}
	}
}

func (d *Database) Exec(query string, args ...any) (sql.Result, error) {
	d.Trace(context.Background(), "Exec", query, args...)
	return d.conn.Exec(query, args...)
}

func (d *Database) ExecCtx(ctx context.Context, query string, args ...any) (sql.Result, error) {
	d.Trace(ctx, "ExecCtx", query, args...)
	return d.conn.ExecCtx(ctx, query, args...)
}

func (d *Database) Prepare(query string) (sqlx.StmtSession, error) {
	d.Trace(context.Background(), "Prepare", query)
	return d.conn.Prepare(query)
}

func (d *Database) PrepareCtx(ctx context.Context, query string) (sqlx.StmtSession, error) {
	d.Trace(ctx, "Prepare", query)
	return d.conn.PrepareCtx(ctx, query)
}

func (d *Database) QueryRow(v any, query string, args ...any) error {
	d.Trace(context.Background(), "QueryRow", query)
	return d.conn.QueryRow(v, query, args...)
}

func (d *Database) QueryRowCtx(ctx context.Context, v any, query string, args ...any) error {
	d.Trace(ctx, "QueryRowCtx", query, args...)
	return d.conn.QueryRowCtx(ctx, v, query, args...)
}

func (d *Database) QueryRowPartial(v any, query string, args ...any) error {
	d.Trace(context.Background(), "QueryRowPartial", query, args...)
	return d.conn.QueryRowPartial(v, query, args...)
}

func (d *Database) QueryRowPartialCtx(ctx context.Context, v any, query string, args ...any) error {
	d.Trace(ctx, "QueryRowPartialCtx", query, args...)
	return d.conn.QueryRowPartialCtx(ctx, v, query, args...)
}

func (d *Database) QueryRows(v any, query string, args ...any) error {
	d.Trace(context.Background(), "QueryRows", query, args...)
	return d.conn.QueryRows(v, query, args...)
}

func (d *Database) QueryRowsCtx(ctx context.Context, v any, query string, args ...any) error {
	d.Trace(ctx, "QueryRowsCtx", query, args...)
	return d.conn.QueryRowsCtx(ctx, v, query, args...)
}

func (d *Database) QueryRowsPartial(v any, query string, args ...any) error {
	d.Trace(context.Background(), "QueryRowsPartial", query, args...)
	return d.conn.QueryRowsPartial(v, query, args...)
}

func (d *Database) QueryRowsPartialCtx(ctx context.Context, v any, query string, args ...any) error {
	d.Trace(ctx, "QueryRowsPartialCtx", query, args...)
	return d.conn.QueryRowsPartialCtx(ctx, v, query, args...)
}

// Transact starts a new transaction and executes it in function method
func (d *Database) Transact(fn func(td *TxDatabase) error) (err error) {
	d.Trace(context.Background(), "Transact", "")
	return d.conn.Transact(func(s sqlx.Session) error {
		td := NewTx(d.dialect, s)
		return fn(td)
	})
}

func (d *Database) TransactCtx(ctx context.Context, fn func(ctx context.Context, td *TxDatabase) error) (err error) {
	d.Trace(ctx, "Transact", "")
	return d.conn.TransactCtx(ctx, func(ctx context.Context, s sqlx.Session) error {
		td := NewTx(d.dialect, s)
		return fn(ctx, td)
	})
}

// A wrapper around a sql.Tx and works the same way as Database
type TxDatabase struct {
	logger  logx.Logger
	dialect string
	session sqlx.Session
}

// Creates a new TxDatabase
func NewTx(dialect string, session sqlx.Session) *TxDatabase {
	return &TxDatabase{dialect: dialect, session: session}
}

// returns this databases dialect
func (td *TxDatabase) Dialect() string {
	return td.dialect
}

// Creates a new Dataset for querying a Database.
func (td *TxDatabase) From(cols ...any) *SelectDataset {
	return newDataset(td.dialect, td.session).From(cols...)
}

func (td *TxDatabase) Select(cols ...any) *SelectDataset {
	return newDataset(td.dialect, td.session).Select(cols...)
}

func (td *TxDatabase) Update(table any) *UpdateDataset {
	return newUpdateDataset(td.dialect, td.session).Table(table)
}

func (td *TxDatabase) Insert(table any) *InsertDataset {
	return newInsertDataset(td.dialect, td.session).Into(table)
}

func (td *TxDatabase) Delete(table any) *DeleteDataset {
	return newDeleteDataset(td.dialect, td.session).From(table)
}

func (td *TxDatabase) Truncate(table ...any) *TruncateDataset {
	return newTruncateDataset(td.dialect, td.session).Table(table...)
}

// Sets the logger
func (td *TxDatabase) Logger(logger logx.Logger) {
	td.logger = logger
}

func (td *TxDatabase) Trace(ctx context.Context, op, sqlString string, args ...any) {
	if td.logger != nil {
		if sqlString != "" {
			if len(args) != 0 {
				td.logger.WithContext(ctx).Infof("[builder - transaction] %s [query:=`%s` args:=%+v] ", op, sqlString, args)
			} else {
				td.logger.WithContext(ctx).Infof("[builder - transaction] %s [query:=`%s`] ", op, sqlString)
			}
		} else {
			td.logger.WithContext(ctx).Infof("[builder - transaction] %s", op)
		}
	}
}

// See Database#Exec
func (td *TxDatabase) Exec(query string, args ...any) (sql.Result, error) {
	ctx := context.Background()
	td.Trace(ctx, "Exec", query, args...)
	return td.session.ExecCtx(ctx, query, args...)
}

// See Database#ExecContext
func (td *TxDatabase) ExecCtx(ctx context.Context, query string, args ...any) (sql.Result, error) {
	td.Trace(ctx, "ExecCtx", query, args...)
	return td.session.ExecCtx(ctx, query, args...)
}

// See Database#Prepare
func (td *TxDatabase) Prepare(query string) (sqlx.StmtSession, error) {
	ctx := context.Background()
	td.Trace(ctx, "Prepare", query)
	return td.session.PrepareCtx(ctx, query)
}

// See Database#PrepareContext
func (td *TxDatabase) PrepareCtx(ctx context.Context, query string) (sqlx.StmtSession, error) {
	td.Trace(ctx, "PrepareCtx", query)
	return td.session.PrepareCtx(ctx, query)
}

// See Database#Query
func (td *TxDatabase) QueryRow(v any, query string, args ...any) error {
	ctx := context.Background()
	td.Trace(ctx, "QueryRow", query, args...)
	return td.session.QueryRowCtx(ctx, v, query, args...)
}

// See Database#QueryContext
func (td *TxDatabase) QueryRowCtx(ctx context.Context, v any, query string, args ...any) error {
	td.Trace(ctx, "QueryRowCtx", query, args...)
	return td.session.QueryRowCtx(ctx, v, query, args...)
}

// See Database#Query
func (td *TxDatabase) QueryRowPartial(v any, query string, args ...any) error {
	ctx := context.Background()
	td.Trace(ctx, "QueryRowPartial", query, args...)
	return td.session.QueryRowPartialCtx(ctx, v, query, args...)
}

// See Database#QueryContext
func (td *TxDatabase) QueryRowPartialCtx(ctx context.Context, v any, query string, args ...any) error {
	td.Trace(ctx, "QueryRowPartialCtx", query, args...)
	return td.session.QueryRowPartialCtx(ctx, v, query, args...)
}

// See Database#Query
func (td *TxDatabase) QueryRows(v any, query string, args ...any) error {
	ctx := context.Background()
	td.Trace(ctx, "QueryRows", query, args...)
	return td.session.QueryRowsCtx(ctx, v, query, args...)
}

// See Database#QueryContext
func (td *TxDatabase) QueryRowsCtx(ctx context.Context, v any, query string, args ...any) error {
	td.Trace(ctx, "QueryRowsCtx", query, args...)
	return td.session.QueryRowsCtx(ctx, v, query, args...)
}

// See Database#Query
func (td *TxDatabase) QueryRowsPartial(v any, query string, args ...any) error {
	ctx := context.Background()
	td.Trace(ctx, "QueryRowsPartial", query, args...)
	return td.session.QueryRowsPartialCtx(context.Background(), v, query, args...)
}

// See Database#QueryContext
func (td *TxDatabase) QueryRowsPartialCtx(ctx context.Context, v any, query string, args ...any) error {
	td.Trace(ctx, "QueryRowsPartialCtx", query, args...)
	return td.session.QueryRowsPartialCtx(ctx, v, query, args...)
}
