package builder

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Tooooommy/builder/v9/exp"
	"github.com/Tooooommy/builder/v9/internal/errors"
	"github.com/Tooooommy/builder/v9/internal/sb"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type InsertDataset struct {
	dialect    SQLDialect
	clauses    exp.InsertClauses
	isPrepared prepared
	executor   sqlx.Session
	err        error
}

var ErrUnsupportedIntoType = errors.New("unsupported table type, a string or identifier expression is required")

// used internally by database to create a database with a specific adapter
func newInsertDataset(d string, executor sqlx.Session) *InsertDataset {
	return &InsertDataset{
		clauses:  exp.NewInsertClauses(),
		dialect:  GetDialect(d),
		executor: executor,
	}
}

// Creates a new InsertDataset for the provided table. Using this method will only allow you
// to create SQL user Database#From to create an InsertDataset with query capabilities
func Insert(table any) *InsertDataset {
	return newInsertDataset("default", nil).Into(table)
}

// Set the parameter interpolation behavior. See examples
//
// prepared: If true the dataset WILL NOT interpolate the parameters.
func (id *InsertDataset) Prepared(prepared bool) *InsertDataset {
	ret := id.copy(id.clauses)
	ret.isPrepared = preparedFromBool(prepared)
	return ret
}

func (id *InsertDataset) IsPrepared() bool {
	return id.isPrepared.Bool()
}

// Sets the adapter used to serialize values and create the SQL statement
func (id *InsertDataset) WithDialect(dl string) *InsertDataset {
	ds := id.copy(id.GetClauses())
	ds.dialect = GetDialect(dl)
	return ds
}

// Returns the current adapter on the dataset
func (id *InsertDataset) Dialect() SQLDialect {
	return id.dialect
}

// Returns the current adapter on the dataset
func (id *InsertDataset) SetDialect(dialect SQLDialect) *InsertDataset {
	cd := id.copy(id.GetClauses())
	cd.dialect = dialect
	return cd
}

func (id *InsertDataset) Expression() exp.Expression {
	return id
}

// Clones the dataset
func (id *InsertDataset) Clone() exp.Expression {
	return id.copy(id.clauses)
}

// Returns the current clauses on the dataset.
func (id *InsertDataset) GetClauses() exp.InsertClauses {
	return id.clauses
}

// used interally to copy the dataset
func (id *InsertDataset) copy(clauses exp.InsertClauses) *InsertDataset {
	return &InsertDataset{
		dialect:    id.dialect,
		clauses:    clauses,
		isPrepared: id.isPrepared,
		executor:   id.executor,
		err:        id.err,
	}
}

// Creates a WITH clause for a common table expression (CTE).
//
// The name will be available to SELECT from in the associated query; and can optionally
// contain a list of column names "name(col1, col2, col3)".
//
// The name will refer to the results of the specified subquery.
func (id *InsertDataset) With(name string, subquery exp.Expression) *InsertDataset {
	return id.copy(id.clauses.CommonTablesAppend(exp.NewCommonTableExpression(false, name, subquery)))
}

// Creates a WITH RECURSIVE clause for a common table expression (CTE)
//
// The name will be available to SELECT from in the associated query; and must
// contain a list of column names "name(col1, col2, col3)" for a recursive clause.
//
// The name will refer to the results of the specified subquery. The subquery for
// a recursive query will always end with a UNION or UNION ALL with a clause that
// refers to the CTE by name.
func (id *InsertDataset) WithRecursive(name string, subquery exp.Expression) *InsertDataset {
	return id.copy(id.clauses.CommonTablesAppend(exp.NewCommonTableExpression(true, name, subquery)))
}

// Sets the table to insert INTO. This return a new dataset with the original table replaced. See examples.
// You can pass in the following.
//
//	string: Will automatically be turned into an identifier
//	Expression: Any valid expression (IdentifierExpression, AliasedExpression, Literal, etc.)
func (id *InsertDataset) Into(into any) *InsertDataset {
	switch t := into.(type) {
	case exp.Expression:
		return id.copy(id.clauses.SetInto(t))
	case string:
		return id.copy(id.clauses.SetInto(exp.ParseIdentifier(t)))
	default:
		panic(ErrUnsupportedIntoType)
	}
}

// Sets the Columns to insert into
func (id *InsertDataset) Cols(cols ...any) *InsertDataset {
	return id.copy(id.clauses.SetCols(exp.NewColumnListExpression(cols...)))
}

// Clears the Columns to insert into
func (id *InsertDataset) ClearCols() *InsertDataset {
	return id.copy(id.clauses.SetCols(nil))
}

// Adds columns to the current list of columns clause. See examples
func (id *InsertDataset) ColsAppend(cols ...any) *InsertDataset {
	return id.copy(id.clauses.ColsAppend(exp.NewColumnListExpression(cols...)))
}

// Adds a subquery to the insert. See examples.
func (id *InsertDataset) FromQuery(from exp.AppendableExpression) *InsertDataset {
	if sds, ok := from.(*SelectDataset); ok {
		if sds.dialect != GetDialect("default") && id.Dialect() != sds.dialect {
			panic(
				fmt.Errorf(
					"incompatible dialects for INSERT (%q) and SELECT (%q)",
					id.dialect.Dialect(), sds.dialect.Dialect(),
				),
			)
		}
		sds.dialect = id.dialect
	}
	return id.copy(id.clauses.SetFrom(from))
}

// Manually set values to insert See examples.
func (id *InsertDataset) Vals(vals ...[]any) *InsertDataset {
	return id.copy(id.clauses.ValsAppend(vals))
}

// Clears the values. See examples.
func (id *InsertDataset) ClearVals() *InsertDataset {
	return id.copy(id.clauses.SetVals(nil))
}

// Insert rows. Rows can be a map, builder.Record or struct. See examples.
func (id *InsertDataset) Rows(rows ...any) *InsertDataset {
	return id.copy(id.clauses.SetRows(rows))
}

// Clears the rows for this insert dataset. See examples.
func (id *InsertDataset) ClearRows() *InsertDataset {
	return id.copy(id.clauses.SetRows(nil))
}

// Adds a RETURNING clause to the dataset if the adapter supports it See examples.
func (id *InsertDataset) Returning(returning ...any) *InsertDataset {
	return id.copy(id.clauses.SetReturning(exp.NewColumnListExpression(returning...)))
}

// Adds an (ON CONFLICT/ON DUPLICATE KEY) clause to the dataset if the dialect supports it. See examples.
func (id *InsertDataset) OnConflict(conflict exp.ConflictExpression) *InsertDataset {
	return id.copy(id.clauses.SetOnConflict(conflict))
}

// Clears the on conflict clause. See example
func (id *InsertDataset) ClearOnConflict() *InsertDataset {
	return id.OnConflict(nil)
}

// Get any error that has been set or nil if no error has been set.
func (id *InsertDataset) Error() error {
	return id.err
}

// Set an error on the dataset if one has not already been set. This error will be returned by a future call to Error
// or as part of ToSQL. This can be used by end users to record errors while building up queries without having to
// track those separately.
func (id *InsertDataset) SetError(err error) *InsertDataset {
	if id.err == nil {
		id.err = err
	}

	return id
}

// Generates the default INSERT statement. If Prepared has been called with true then the statement will not be
// interpolated. See examples. When using structs you may specify a column to be skipped in the insert, (e.g. id) by
// specifying a builder tag with `skipinsert`
//
//	type Item struct{
//	   Id   uint32 `db:"id" builder:"skipinsert"`
//	   Name string `db:"name"`
//	}
//
// rows: variable number arguments of either map[string]interface, Record, struct, or a single slice argument of the
// accepted types.
//
// Errors:
//   - There is no INTO clause
//   - Different row types passed in, all rows must be of the same type
//   - Maps with different numbers of K/V pairs
//   - Rows of different lengths, (i.e. (Record{"name": "a"}, Record{"name": "a", "age": 10})
//   - Error generating SQL
func (id *InsertDataset) ToSQL() (sql string, params []any, err error) {
	return id.insertSQLBuilder().ToSQL()
}

// Appends this Dataset's INSERT statement to the SQLBuilder
// This is used internally when using inserts in CTEs
func (id *InsertDataset) AppendSQL(b sb.SQLBuilder) {
	if id.err != nil {
		b.SetError(id.err)
		return
	}
	id.dialect.ToInsertSQL(b, id.GetClauses())
}

func (id *InsertDataset) GetAs() exp.IdentifierExpression {
	return id.clauses.Alias()
}

// Sets the alias for this dataset. This is typically used when using a Dataset as MySQL upsert
func (id *InsertDataset) As(alias string) *InsertDataset {
	return id.copy(id.clauses.SetAlias(T(alias)))
}

func (id *InsertDataset) ReturnsColumns() bool {
	return id.clauses.HasReturning()
}

// Generates the INSERT sql, and returns an QueryExecutor struct with the sql set to the INSERT statement
//
//	db.Insert("test").Rows(Record{"name":"Bob"}).Executor().Exec()

func (id *InsertDataset) Exec() (sql.Result, error) {
	return id.ExecCtx(context.Background())
}

func (id *InsertDataset) ExecCtx(ctx context.Context) (sql.Result, error) {
	query, args, err := id.buildSQL()
	if err != nil {
		return nil, err
	}
	return id.executor.ExecCtx(ctx, query, args...)
}

func (id *InsertDataset) QueryRow(v any) error {
	return id.QueryRowCtx(context.Background(), v)
}

func (id *InsertDataset) QueryRowCtx(ctx context.Context, v any) error {
	query, args, err := id.buildSQL()
	if err != nil {
		return err
	}
	return id.executor.QueryRowCtx(ctx, v, query, args...)
}

func (id *InsertDataset) QueryRowPartial(v any) error {
	return id.QueryRowPartialCtx(context.Background(), v)
}

func (id *InsertDataset) QueryRowPartialCtx(ctx context.Context, v any) error {
	query, args, err := id.buildSQL()
	if err != nil {
		return err
	}
	return id.executor.QueryRowPartialCtx(ctx, v, query, args...)
}

func (id *InsertDataset) QueryRows(v any) error {
	return id.QueryRowsCtx(context.Background(), v)
}

func (id *InsertDataset) QueryRowsCtx(ctx context.Context, v any) error {
	query, args, err := id.buildSQL()
	if err != nil {
		return err
	}
	return id.executor.QueryRowsCtx(ctx, v, query, args...)
}

func (id *InsertDataset) QueryRowsPartial(v any) error {
	return id.QueryRowsPartialCtx(context.Background(), v)
}

func (id *InsertDataset) QueryRowsPartialCtx(ctx context.Context, v any) error {
	query, args, err := id.buildSQL()
	if err != nil {
		return err
	}
	return id.executor.QueryRowsPartialCtx(ctx, v, query, args...)
}

func (id *InsertDataset) buildSQL() (string, []any, error) {
	if id.executor == nil {
		return "", nil, ErrExecutorNotFoundError
	}
	return id.insertSQLBuilder().ToSQL()
}

func (id *InsertDataset) insertSQLBuilder() sb.SQLBuilder {
	buf := sb.NewSQLBuilder(id.isPrepared.Bool())
	if id.err != nil {
		return buf.SetError(id.err)
	}
	id.dialect.ToInsertSQL(buf, id.clauses)
	return buf
}
