package mysql_test

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Tooooommy/builder/v9"
	"github.com/Tooooommy/builder/v9/dialect/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/suite"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

const (
	dropTable   = "DROP TABLE IF EXISTS `entry`;"
	createTable = "CREATE  TABLE `entry` (" +
		"`id` INT NOT NULL AUTO_INCREMENT ," +
		"`int` INT NOT NULL UNIQUE," +
		"`float` FLOAT NOT NULL ," +
		"`string` VARCHAR(255) NOT NULL ," +
		"`time` DATETIME NOT NULL ," +
		"`bool` TINYINT NOT NULL ," +
		"`bytes` BLOB NOT NULL ," +
		"PRIMARY KEY (`id`) );"
	insertDefaultReords = "INSERT INTO `entry` (`int`, `float`, `string`, `time`, `bool`, `bytes`) VALUES" +
		"(0, 0.000000, '0.000000', '2015-02-22 18:19:55', TRUE,  '0.000000')," +
		"(1, 0.100000, '0.100000', '2015-02-22 19:19:55', FALSE, '0.100000')," +
		"(2, 0.200000, '0.200000', '2015-02-22 20:19:55', TRUE,  '0.200000')," +
		"(3, 0.300000, '0.300000', '2015-02-22 21:19:55', FALSE, '0.300000')," +
		"(4, 0.400000, '0.400000', '2015-02-22 22:19:55', TRUE,  '0.400000')," +
		"(5, 0.500000, '0.500000', '2015-02-22 23:19:55', FALSE, '0.500000')," +
		"(6, 0.600000, '0.600000', '2015-02-23 00:19:55', TRUE,  '0.600000')," +
		"(7, 0.700000, '0.700000', '2015-02-23 01:19:55', FALSE, '0.700000')," +
		"(8, 0.800000, '0.800000', '2015-02-23 02:19:55', TRUE,  '0.800000')," +
		"(9, 0.900000, '0.900000', '2015-02-23 03:19:55', FALSE, '0.900000');"
)

const defaultDBURI = "tommy:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=true"

type (
	mysqlTest struct {
		suite.Suite
		db *builder.Database
	}
	entry struct {
		ID     uint32    `db:"id" builder:"skipinsert,skipupdate"`
		Int    int       `db:"int"`
		Float  float64   `db:"float"`
		String string    `db:"string"`
		Time   time.Time `db:"time"`
		Bool   bool      `db:"bool"`
		Bytes  []byte    `db:"bytes"`
	}
	entryTestCase struct {
		ds    *builder.SelectDataset
		len   int
		check func(entry entry, index int)
		err   string
	}
)

func (mt *mysqlTest) SetupSuite() {
	dbURI := os.Getenv("MYSQL_URI")
	if dbURI == "" {
		dbURI = defaultDBURI
	}
	db, err := sql.Open("mysql", dbURI)
	if err != nil {
		panic(err.Error())
	}
	conn := sqlx.NewSqlConnFromDB(db)
	mt.db = builder.New("mysql", conn)
}

func (mt *mysqlTest) assertSQL(cases ...sqlTestCase) {
	for i, c := range cases {
		actualSQL, actualArgs, err := c.ds.ToSQL()
		if c.err == "" {
			mt.NoError(err, "test case %d failed", i)
		} else {
			mt.EqualError(err, c.err, "test case %d failed", i)
		}
		mt.Equal(c.sql, actualSQL, "test case %d failed", i)
		if c.isPrepared && c.args != nil || len(c.args) > 0 {
			mt.Equal(c.args, actualArgs, "test case %d failed", i)
		} else {
			mt.Empty(actualArgs, "test case %d failed", i)
		}
	}
}

func (mt *mysqlTest) assertEntries(cases ...entryTestCase) {
	for i, c := range cases {
		var entries []entry
		err := c.ds.QueryRows(&entries)
		if c.err == "" {
			mt.NoError(err, "test case %d failed", i)
		} else {
			mt.EqualError(err, c.err, "test case %d failed", i)
		}
		mt.Len(entries, c.len)
		for index, entry := range entries {
			c.check(entry, index)
		}
	}
}

func (mt *mysqlTest) SetupTest() {
	if _, err := mt.db.Exec(dropTable); err != nil {
		panic(err)
	}
	if _, err := mt.db.Exec(createTable); err != nil {
		panic(err)
	}
	if _, err := mt.db.Exec(insertDefaultReords); err != nil {
		panic(err)
	}
}

func (mt *mysqlTest) TestToSQL() {
	ds := mt.db.From("entry")
	mt.assertSQL(
		sqlTestCase{ds: ds.Select("id", "float", "string", "time", "bool"), sql: "SELECT `id`, `float`, `string`, `time`, `bool` FROM `entry`"},
		sqlTestCase{ds: ds.Where(builder.C("int").Eq(10)), sql: "SELECT * FROM `entry` WHERE (`int` = 10)"},
		sqlTestCase{
			ds:  ds.Prepared(true).Where(builder.L("? = ?", builder.C("int"), 10)),
			sql: "SELECT * FROM `entry` WHERE `int` = ?", args: []any{int64(10)},
		},
	)
}

func (mt *mysqlTest) TestQuery() {
	ds := mt.db.From("entry")
	floatVal := float64(0)
	baseDate, err := time.Parse(
		"2006-01-02 15:04:05",
		"2015-02-22 18:19:55",
	)
	mt.NoError(err)
	mt.assertEntries(
		entryTestCase{ds: ds.Order(builder.C("id").Asc()), len: 10, check: func(entry entry, index int) {
			f := fmt.Sprintf("%f", floatVal)
			mt.Equal(uint32(index+1), entry.ID)
			mt.Equal(index, entry.Int)
			mt.Equal(f, fmt.Sprintf("%f", entry.Float))
			mt.Equal(f, entry.String)
			mt.Equal([]byte(f), entry.Bytes)
			mt.Equal(index%2 == 0, entry.Bool)
			mt.Equal(baseDate.Add(time.Duration(index)*time.Hour).Unix(), entry.Time.Unix())
			floatVal += float64(0.1)
		}},
		entryTestCase{ds: ds.Where(builder.C("bool").IsTrue()).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			mt.True(entry.Bool)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Gt(4)).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			mt.True(entry.Int > 4)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Gte(5)).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			mt.True(entry.Int >= 5)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Lt(5)).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			mt.True(entry.Int < 5)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Lte(4)).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			mt.True(entry.Int <= 4)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Between(builder.Range(3, 6))).Order(builder.C("id").Asc()), len: 4, check: func(entry entry, _ int) {
			mt.True(entry.Int >= 3)
			mt.True(entry.Int <= 6)
		}},
		entryTestCase{ds: ds.Where(builder.C("string").Eq("0.100000")).Order(builder.C("id").Asc()), len: 1, check: func(entry entry, _ int) {
			mt.Equal(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(builder.C("string").Like("0.1%")).Order(builder.C("id").Asc()), len: 1, check: func(entry entry, _ int) {
			mt.Equal(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(builder.C("string").NotLike("0.1%")).Order(builder.C("id").Asc()), len: 9, check: func(entry entry, _ int) {
			mt.NotEqual(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(builder.C("string").IsNull()).Order(builder.C("id").Asc()), len: 0, check: func(entry entry, _ int) {
			mt.Fail("Should not have returned any records")
		}},
	)
}

func (mt *mysqlTest) TestQuery_Prepared() {
	ds := mt.db.From("entry").Prepared(true)
	floatVal := float64(0)
	baseDate, err := time.Parse(
		"2006-01-02 15:04:05",
		"2015-02-22 18:19:55",
	)
	mt.NoError(err)
	mt.assertEntries(
		entryTestCase{ds: ds.Order(builder.C("id").Asc()), len: 10, check: func(entry entry, index int) {
			f := fmt.Sprintf("%f", floatVal)
			mt.Equal(uint32(index+1), entry.ID)
			mt.Equal(index, entry.Int)
			mt.Equal(f, fmt.Sprintf("%f", entry.Float))
			mt.Equal(f, entry.String)
			mt.Equal([]byte(f), entry.Bytes)
			mt.Equal(index%2 == 0, entry.Bool)
			mt.Equal(baseDate.Add(time.Duration(index)*time.Hour).Unix(), entry.Time.Unix())
			floatVal += float64(0.1)
		}},
		entryTestCase{ds: ds.Where(builder.C("bool").IsTrue()).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			mt.True(entry.Bool)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Gt(4)).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			mt.True(entry.Int > 4)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Gte(5)).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			mt.True(entry.Int >= 5)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Lt(5)).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			mt.True(entry.Int < 5)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Lte(4)).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			mt.True(entry.Int <= 4)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Between(builder.Range(3, 6))).Order(builder.C("id").Asc()), len: 4, check: func(entry entry, _ int) {
			mt.True(entry.Int >= 3)
			mt.True(entry.Int <= 6)
		}},
		entryTestCase{ds: ds.Where(builder.C("string").Eq("0.100000")).Order(builder.C("id").Asc()), len: 1, check: func(entry entry, _ int) {
			mt.Equal(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(builder.C("string").Like("0.1%")).Order(builder.C("id").Asc()), len: 1, check: func(entry entry, _ int) {
			mt.Equal(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(builder.C("string").NotLike("0.1%")).Order(builder.C("id").Asc()), len: 9, check: func(entry entry, _ int) {
			mt.NotEqual(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(builder.C("string").IsNull()).Order(builder.C("id").Asc()), len: 0, check: func(entry entry, _ int) {
			mt.Fail("Should not have returned any records")
		}},
	)
}

func (mt *mysqlTest) TestQuery_ValueExpressions() {
	type wrappedEntry struct {
		entry
		BoolValue bool `db:"bool_value"`
	}
	expectedDate, err := time.Parse("2006-01-02 15:04:05", "2015-02-22 19:19:55")
	mt.NoError(err)
	ds := mt.db.From("entry").Select(builder.Star(), builder.V(true).As("bool_value")).Where(builder.Ex{"int": 1})
	var we wrappedEntry
	err = ds.QueryRow(&we)
	mt.NoError(err)
	mt.Equal(wrappedEntry{
		entry{2, 1, 0.100000, "0.100000", expectedDate, false, []byte("0.100000")},
		true,
	}, we)
}

func (mt *mysqlTest) TestCount() {
	ds := mt.db.From("entry")
	count, err := ds.Count()
	mt.NoError(err)
	mt.Equal(int64(10), count)
	count, err = ds.Where(builder.C("int").Gt(4)).Count()
	mt.NoError(err)
	mt.Equal(int64(5), count)
	count, err = ds.Where(builder.C("int").Gte(4)).Count()
	mt.NoError(err)
	mt.Equal(int64(6), count)
	count, err = ds.Where(builder.C("string").Like("0.1%")).Count()
	mt.NoError(err)
	mt.Equal(int64(1), count)
	count, err = ds.Where(builder.C("string").IsNull()).Count()
	mt.NoError(err)
	mt.Equal(int64(0), count)
}

func (mt *mysqlTest) TestInsert() {
	ds := mt.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	_, err := ds.Insert().Rows(e).Exec()
	mt.NoError(err)

	var insertedEntry entry
	err = ds.Where(builder.C("int").Eq(10)).QueryRow(&insertedEntry)
	mt.NoError(err)
	mt.True(insertedEntry.ID > 0)

	entries := []entry{
		{Int: 11, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")},
		{Int: 12, Float: 1.200000, String: "1.200000", Time: now, Bool: true, Bytes: []byte("1.200000")},
		{Int: 13, Float: 1.300000, String: "1.300000", Time: now, Bool: false, Bytes: []byte("1.300000")},
		{Int: 14, Float: 1.400000, String: "1.400000", Time: now, Bool: true, Bytes: []byte("1.400000")},
	}
	_, err = ds.Insert().Rows(entries).Exec()
	mt.NoError(err)

	var newEntries []entry
	mt.NoError(ds.Where(builder.C("int").In([]uint32{11, 12, 13, 14})).QueryRows(&newEntries))
	mt.Len(newEntries, 4)
	for i, e := range newEntries {
		mt.Equal(entries[i].Int, e.Int)
		mt.Equal(entries[i].Float, e.Float)
		mt.Equal(entries[i].String, e.String)
		mt.Equal(entries[i].Time.UTC().Format(mysql.DialectOptions().TimeFormat), e.Time.Format(mysql.DialectOptions().TimeFormat))
		mt.Equal(entries[i].Bool, e.Bool)
		mt.Equal(entries[i].Bytes, e.Bytes)
	}

	_, err = ds.Insert().Rows(
		entry{Int: 15, Float: 1.500000, String: "1.500000", Time: now, Bool: false, Bytes: []byte("1.500000")},
		entry{Int: 16, Float: 1.600000, String: "1.600000", Time: now, Bool: true, Bytes: []byte("1.600000")},
		entry{Int: 17, Float: 1.700000, String: "1.700000", Time: now, Bool: false, Bytes: []byte("1.700000")},
		entry{Int: 18, Float: 1.800000, String: "1.800000", Time: now, Bool: true, Bytes: []byte("1.800000")},
	).Exec()
	mt.NoError(err)

	newEntries = newEntries[0:0]
	mt.NoError(ds.Where(builder.C("int").In([]uint32{15, 16, 17, 18})).QueryRows(&newEntries))
	mt.Len(newEntries, 4)
}

func (mt *mysqlTest) TestInsertReturning() {
	ds := mt.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	err := ds.Insert().Rows(e).Returning(builder.Star()).QueryRow(&e)
	mt.Error(err)
}

func (mt *mysqlTest) TestUpdate() {
	ds := mt.db.From("entry")
	var e entry
	err := ds.Where(builder.C("int").Eq(9)).Select("id").QueryRowPartial(&e)
	mt.NoError(err)
	e.Int = 11
	_, err = ds.Where(builder.C("id").Eq(e.ID)).Update().Set(e).Exec()
	mt.NoError(err)

	count, err := ds.Where(builder.C("int").Eq(11)).Count()
	mt.NoError(err)
	mt.Equal(int64(1), count)
}

func (mt *mysqlTest) TestUpdateReturning() {
	ds := mt.db.From("entry")
	var id uint32
	err := ds.Where(builder.C("int").Eq(11)).
		Update().
		Set(builder.Record{"int": 9}).
		Returning("id").
		QueryRowPartial(&id)
	mt.Error(err)
	mt.EqualError(err, "builder: dialect does not support RETURNING clause [dialect=mysql]")
}

func (mt *mysqlTest) TestDelete() {
	ds := mt.db.From("entry")
	var e entry
	err := ds.Where(builder.C("int").Eq(9)).Select("id").QueryRowPartial(&e)
	mt.NoError(err)
	_, err = ds.Where(builder.C("id").Eq(e.ID)).Delete().Exec()
	mt.NoError(err)

	count, err := ds.Count()
	mt.NoError(err)
	mt.Equal(int64(9), count)

	var ids []uint32
	err = ds.Where(builder.C("id").Eq(e.ID)).Select("id").QueryRowsPartial(&ids)
	mt.NoError(err)

	e = entry{}
	err = ds.Where(builder.C("int").Eq(8)).QueryRow(&e)
	mt.NoError(err)
	mt.NotEqual(0, e.ID)

	var id = 0
	err = ds.Where(builder.C("id").Eq(e.ID)).Delete().Returning("id").QueryRowPartial(&id)
	mt.EqualError(err, "builder: dialect does not support RETURNING clause [dialect=mysql]")
}

func (mt *mysqlTest) TestInsertIgnore() {
	ds := mt.db.From("entry")
	now := time.Now()

	// insert one
	entries := []entry{
		{Int: 8, Float: 6.100000, String: "6.100000", Time: now, Bytes: []byte("6.100000")},
		{Int: 9, Float: 7.200000, String: "7.200000", Time: now, Bytes: []byte("7.200000")},
		{Int: 10, Float: 7.200000, String: "7.200000", Time: now, Bytes: []byte("7.200000")},
	}
	_, err := ds.Insert().Rows(entries).OnConflict(builder.DoNothing()).Exec()
	mt.NoError(err)

	count, err := ds.Count()
	mt.NoError(err)
	mt.Equal(count, int64(11))
}

func (mt *mysqlTest) TestInsert_OnConflict() {
	ds := mt.db.From("entry")
	now := time.Now()

	// insert
	e := entry{Int: 10, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")}
	_, err := ds.Insert().Rows(e).OnConflict(builder.DoNothing()).Exec()
	mt.NoError(err)

	// duplicate
	e = entry{Int: 10, Float: 2.100000, String: "2.100000", Time: now.Add(time.Hour * 100), Bool: false, Bytes: []byte("2.100000")}
	_, err = ds.Insert().Rows(e).OnConflict(builder.DoNothing()).Exec()
	mt.NoError(err)

	// update
	var entryActual entry
	e2 := entry{Int: 10, String: "2.000000"}
	_, err = ds.Insert().
		Rows(e2).
		OnConflict(builder.DoUpdate("int", builder.Record{"string": "upsert"})).
		Exec()
	mt.NoError(err)
	err = ds.Where(builder.C("int").Eq(10)).QueryRow(&entryActual)
	mt.NoError(err)
	mt.Equal("upsert", entryActual.String)

	// update where should error
	entries := []entry{
		{Int: 8, Float: 6.100000, String: "6.100000", Time: now, Bytes: []byte("6.100000")},
		{Int: 9, Float: 7.200000, String: "7.200000", Time: now, Bytes: []byte("7.200000")},
	}
	_, err = ds.Insert().
		Rows(entries).
		OnConflict(builder.DoUpdate("int", builder.Record{"string": "upsert"}).Where(builder.C("int").Eq(9))).
		Exec()
	mt.EqualError(err, "builder: dialect does not support upsert with where clause [dialect=mysql]")
}

func (mt *mysqlTest) TestWindowFunction() {
	var version string
	err := mt.db.Select(builder.Func("version")).QueryRowPartial(&version)
	mt.NoError(err)

	fields := strings.Split(version, ".")
	mt.True(len(fields) > 0)
	major, err := strconv.Atoi(fields[0])
	mt.NoError(err)
	if major < 8 {
		//nolint:forbidigo
		fmt.Printf("SKIPPING MYSQL WINDOW FUNCTION TEST BECAUSE VERSION IS < 8 [mysql_version:=%d]\n", major)
		return
	}

	ds := mt.db.From("entry").
		Select("int", builder.ROW_NUMBER().OverName(builder.I("w")).As("id")).
		Window(builder.W("w").OrderBy(builder.I("int").Desc()))

	var entries []entry
	err = ds.WithDialect("mysql8").QueryRowsPartial(&entries)
	mt.NoError(err)

	mt.Equal([]entry{
		{Int: 9, ID: 1},
		{Int: 8, ID: 2},
		{Int: 7, ID: 3},
		{Int: 6, ID: 4},
		{Int: 5, ID: 5},
		{Int: 4, ID: 6},
		{Int: 3, ID: 7},
		{Int: 2, ID: 8},
		{Int: 1, ID: 9},
		{Int: 0, ID: 10},
	}, entries)

	err = ds.WithDialect("mysql").QueryRowsPartial(&entries)
	mt.Error(err, "builder: adapter does not support window function clause")
}

func (mt *mysqlTest) TestInsertFromSelect() {
	ds := mt.db.From("entry")

	subquery := builder.Select(
		builder.V(11),
		builder.V(11),
		builder.C("float"),
		builder.C("string"),
		builder.C("time"),
		builder.C("bool"),
		builder.C("bytes"),
	).From(builder.T("entry")).Where(builder.C("int").Eq(9))

	query := ds.Insert().Cols().FromQuery(subquery)
	_, _, err := query.ToSQL()

	mt.NoError(err)
	_, err = query.Exec()
	mt.NoError(err)
}

func TestMysqlSuite(t *testing.T) {
	suite.Run(t, new(mysqlTest))
}
