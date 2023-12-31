package postgres_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Tooooommy/builder/v9"
	"github.com/zeromicro/go-zero/core/stores/sqlx"

	"github.com/lib/pq"
	"github.com/stretchr/testify/suite"
)

const schema = `
        DROP TABLE IF EXISTS "entry";
        CREATE  TABLE "entry" (
            "id" SERIAL PRIMARY KEY NOT NULL,
            "int" INT NOT NULL UNIQUE,
            "float" NUMERIC NOT NULL ,
            "string" VARCHAR(45) NOT NULL ,
            "time" TIMESTAMP NOT NULL ,
            "bool" BOOL NOT NULL ,
            "bytes" VARCHAR(45) NOT NULL);
        INSERT INTO "entry" ("int", "float", "string", "time", "bool", "bytes") VALUES
            (0, 0.000000, '0.000000', '2015-02-22T18:19:55.000000000-00:00', TRUE,  '0.000000'),
            (1, 0.100000, '0.100000', '2015-02-22T19:19:55.000000000-00:00', FALSE, '0.100000'),
            (2, 0.200000, '0.200000', '2015-02-22T20:19:55.000000000-00:00', TRUE,  '0.200000'),
            (3, 0.300000, '0.300000', '2015-02-22T21:19:55.000000000-00:00', FALSE, '0.300000'),
            (4, 0.400000, '0.400000', '2015-02-22T22:19:55.000000000-00:00', TRUE,  '0.400000'),
            (5, 0.500000, '0.500000', '2015-02-22T23:19:55.000000000-00:00', FALSE, '0.500000'),
            (6, 0.600000, '0.600000', '2015-02-23T00:19:55.000000000-00:00', TRUE,  '0.600000'),
            (7, 0.700000, '0.700000', '2015-02-23T01:19:55.000000000-00:00', FALSE, '0.700000'),
            (8, 0.800000, '0.800000', '2015-02-23T02:19:55.000000000-00:00', TRUE,  '0.800000'),
            (9, 0.900000, '0.900000', '2015-02-23T03:19:55.000000000-00:00', FALSE, '0.900000');
    `

const defaultDBURI = "postgres://postgres:@localhost:5435/builderpostgres?sslmode=disable"

type (
	postgresTest struct {
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

func (pt *postgresTest) assertEntries(cases ...entryTestCase) {
	for i, c := range cases {
		var entries []entry
		err := c.ds.QueryRows(&entries)
		if c.err == "" {
			pt.NoError(err, "test case %d failed", i)
		} else {
			pt.EqualError(err, c.err, "test case %d failed", i)
		}
		pt.Len(entries, c.len)
		for index, entry := range entries {
			c.check(entry, index)
		}
	}
}

func (pt *postgresTest) SetupSuite() {
	dbURI := os.Getenv("PG_URI")
	if dbURI == "" {
		dbURI = defaultDBURI
	}
	uri, err := pq.ParseURL(dbURI)
	if err != nil {
		panic(err)
	}
	db, err := sql.Open("postgres", uri)
	if err != nil {
		panic(err)
	}
	conn := sqlx.NewSqlConnFromDB(db)
	pt.db = builder.New("postgres", conn)
}

func (pt *postgresTest) SetupTest() {
	if _, err := pt.db.Exec(schema); err != nil {
		panic(err)
	}
}

func (pt *postgresTest) TestToSQL() {
	ds := pt.db.From("entry")
	s, _, err := ds.Select("id", "float", "string", "time", "bool").ToSQL()
	pt.NoError(err)
	pt.Equal(`SELECT "id", "float", "string", "time", "bool" FROM "entry"`, s)

	s, _, err = ds.Where(builder.C("int").Eq(10)).ToSQL()
	pt.NoError(err)
	pt.Equal(`SELECT * FROM "entry" WHERE ("int" = 10)`, s)

	s, args, err := ds.Prepared(true).Where(builder.L("? = ?", builder.C("int"), 10)).ToSQL()
	pt.NoError(err)
	pt.Equal([]any{int64(10)}, args)
	pt.Equal(`SELECT * FROM "entry" WHERE "int" = $1`, s)
}

func (pt *postgresTest) TestQuery() {
	ds := pt.db.From("entry")
	floatVal := float64(0)
	baseDate, err := time.Parse(time.RFC3339Nano, "2015-02-22T18:19:55.000000000-00:00")
	pt.NoError(err)
	baseDate = baseDate.UTC()
	pt.assertEntries(
		entryTestCase{ds: ds.Order(builder.C("id").Asc()), len: 10, check: func(entry entry, index int) {
			f := fmt.Sprintf("%f", floatVal)
			pt.Equal(uint32(index+1), entry.ID)
			pt.Equal(index, entry.Int)
			pt.Equal(f, fmt.Sprintf("%f", entry.Float))
			pt.Equal(f, entry.String)
			pt.Equal([]byte(f), entry.Bytes)
			pt.Equal(index%2 == 0, entry.Bool)
			pt.Equal(baseDate.Add(time.Duration(index)*time.Hour).Unix(), entry.Time.Unix())
			floatVal += float64(0.1)
		}},
		entryTestCase{ds: ds.Where(builder.C("bool").IsTrue()).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			pt.True(entry.Bool)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Gt(4)).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			pt.True(entry.Int > 4)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Gte(5)).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			pt.True(entry.Int >= 5)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Lt(5)).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			pt.True(entry.Int < 5)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Lte(4)).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			pt.True(entry.Int <= 4)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Between(builder.Range(3, 6))).Order(builder.C("id").Asc()), len: 4, check: func(entry entry, _ int) {
			pt.True(entry.Int >= 3)
			pt.True(entry.Int <= 6)
		}},
		entryTestCase{ds: ds.Where(builder.C("string").Eq("0.100000")).Order(builder.C("id").Asc()), len: 1, check: func(entry entry, _ int) {
			pt.Equal(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(builder.C("string").Like("0.1%")).Order(builder.C("id").Asc()), len: 1, check: func(entry entry, _ int) {
			pt.Equal(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(builder.C("string").NotLike("0.1%")).Order(builder.C("id").Asc()), len: 9, check: func(entry entry, _ int) {
			pt.NotEqual(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(builder.C("string").IsNull()).Order(builder.C("id").Asc()), len: 0, check: func(entry entry, _ int) {
			pt.Fail("Should not have returned any records")
		}},
	)
}

func (pt *postgresTest) TestQuery_Prepared() {
	ds := pt.db.From("entry").Prepared(true)
	floatVal := float64(0)
	baseDate, err := time.Parse(time.RFC3339Nano, "2015-02-22T18:19:55.000000000-00:00")
	pt.NoError(err)
	baseDate = baseDate.UTC()
	pt.assertEntries(
		entryTestCase{ds: ds.Order(builder.C("id").Asc()), len: 10, check: func(entry entry, index int) {
			f := fmt.Sprintf("%f", floatVal)
			pt.Equal(uint32(index+1), entry.ID)
			pt.Equal(index, entry.Int)
			pt.Equal(f, fmt.Sprintf("%f", entry.Float))
			pt.Equal(f, entry.String)
			pt.Equal([]byte(f), entry.Bytes)
			pt.Equal(index%2 == 0, entry.Bool)
			pt.Equal(baseDate.Add(time.Duration(index)*time.Hour).Unix(), entry.Time.Unix())
			floatVal += float64(0.1)
		}},
		entryTestCase{ds: ds.Where(builder.C("bool").IsTrue()).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			pt.True(entry.Bool)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Gt(4)).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			pt.True(entry.Int > 4)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Gte(5)).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			pt.True(entry.Int >= 5)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Lt(5)).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			pt.True(entry.Int < 5)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Lte(4)).Order(builder.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			pt.True(entry.Int <= 4)
		}},
		entryTestCase{ds: ds.Where(builder.C("int").Between(builder.Range(3, 6))).Order(builder.C("id").Asc()), len: 4, check: func(entry entry, _ int) {
			pt.True(entry.Int >= 3)
			pt.True(entry.Int <= 6)
		}},
		entryTestCase{ds: ds.Where(builder.C("string").Eq("0.100000")).Order(builder.C("id").Asc()), len: 1, check: func(entry entry, _ int) {
			pt.Equal(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(builder.C("string").Like("0.1%")).Order(builder.C("id").Asc()), len: 1, check: func(entry entry, _ int) {
			pt.Equal(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(builder.C("string").NotLike("0.1%")).Order(builder.C("id").Asc()), len: 9, check: func(entry entry, _ int) {
			pt.NotEqual(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(builder.C("string").IsNull()).Order(builder.C("id").Asc()), len: 0, check: func(entry entry, _ int) {
			pt.Fail("Should not have returned any records")
		}},
	)
}

func (pt *postgresTest) TestQuery_ValueExpressions() {
	type wrappedEntry struct {
		entry
		BoolValue bool `db:"bool_value"`
	}
	expectedDate, err := time.Parse(time.RFC3339Nano, "2015-02-22T19:19:55.000000000-00:00")
	pt.NoError(err)
	ds := pt.db.From("entry").Select(builder.Star(), builder.V(true).As("bool_value")).Where(builder.Ex{"int": 1})
	var we wrappedEntry
	err = ds.QueryRow(&we)
	pt.NoError(err)
	pt.Equal(1, we.Int)
	pt.Equal(0.100000, we.Float)
	pt.Equal("0.100000", we.String)
	pt.Equal(expectedDate.Unix(), we.Time.Unix())
	pt.Equal(false, we.Bool)
	pt.Equal([]byte("0.100000"), we.Bytes)
	pt.True(we.BoolValue)
}

func (pt *postgresTest) TestCount() {
	ds := pt.db.From("entry")
	count, err := ds.Count()
	pt.NoError(err)
	pt.Equal(int64(10), count)
	count, err = ds.Where(builder.C("int").Gt(4)).Count()
	pt.NoError(err)
	pt.Equal(int64(5), count)
	count, err = ds.Where(builder.C("int").Gte(4)).Count()
	pt.NoError(err)
	pt.Equal(int64(6), count)
	count, err = ds.Where(builder.C("string").Like("0.1%")).Count()
	pt.NoError(err)
	pt.Equal(int64(1), count)
	count, err = ds.Where(builder.C("string").IsNull()).Count()
	pt.NoError(err)
	pt.Equal(int64(0), count)
}

func (pt *postgresTest) TestInsert() {
	ds := pt.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	_, err := ds.Insert().Rows(e).Exec()
	pt.NoError(err)

	var insertedEntry entry
	err = ds.Where(builder.C("int").Eq(10)).QueryRow(&insertedEntry)
	pt.NoError(err)
	pt.True(insertedEntry.ID > 0)

	entries := []entry{
		{Int: 11, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")},
		{Int: 12, Float: 1.200000, String: "1.200000", Time: now, Bool: true, Bytes: []byte("1.200000")},
		{Int: 13, Float: 1.300000, String: "1.300000", Time: now, Bool: false, Bytes: []byte("1.300000")},
		{Int: 14, Float: 1.400000, String: "1.400000", Time: now, Bool: true, Bytes: []byte("1.400000")},
	}
	_, err = ds.Insert().Rows(entries).Exec()
	pt.NoError(err)

	var newEntries []entry

	pt.NoError(ds.Where(builder.C("int").In([]uint32{11, 12, 13, 14})).QueryRows(&newEntries))
	pt.Len(newEntries, 4)
	for i, e := range newEntries {
		pt.Equal(entries[i].Int, e.Int)
		pt.Equal(entries[i].Float, e.Float)
		pt.Equal(entries[i].String, e.String)
		pt.Equal(entries[i].Time.Unix(), e.Time.Unix())
		pt.Equal(entries[i].Bool, e.Bool)
		pt.Equal(entries[i].Bytes, e.Bytes)
	}

	_, err = ds.Insert().Rows(
		entry{Int: 15, Float: 1.500000, String: "1.500000", Time: now, Bool: false, Bytes: []byte("1.500000")},
		entry{Int: 16, Float: 1.600000, String: "1.600000", Time: now, Bool: true, Bytes: []byte("1.600000")},
		entry{Int: 17, Float: 1.700000, String: "1.700000", Time: now, Bool: false, Bytes: []byte("1.700000")},
		entry{Int: 18, Float: 1.800000, String: "1.800000", Time: now, Bool: true, Bytes: []byte("1.800000")},
	).Exec()
	pt.NoError(err)

	newEntries = newEntries[0:0]
	pt.NoError(ds.Where(builder.C("int").In([]uint32{15, 16, 17, 18})).QueryRows(&newEntries))
	pt.Len(newEntries, 4)
}

func (pt *postgresTest) TestInsertReturning() {
	ds := pt.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	err := ds.Insert().Rows(e).Returning(builder.Star()).QueryRow(&e)
	pt.NoError(err)
	pt.True(e.ID > 0)

	var ids []uint32
	pt.NoError(ds.Insert().Rows([]entry{
		{Int: 11, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")},
		{Int: 12, Float: 1.200000, String: "1.200000", Time: now, Bool: true, Bytes: []byte("1.200000")},
		{Int: 13, Float: 1.300000, String: "1.300000", Time: now, Bool: false, Bytes: []byte("1.300000")},
		{Int: 14, Float: 1.400000, String: "1.400000", Time: now, Bool: true, Bytes: []byte("1.400000")},
	}).Returning("id").QueryRowsPartial(&ids))
	pt.Len(ids, 4)
	for _, id := range ids {
		pt.True(id > 0)
	}

	var ints []int64
	pt.NoError(ds.Insert().Rows(
		entry{Int: 15, Float: 1.500000, String: "1.500000", Time: now, Bool: false, Bytes: []byte("1.500000")},
		entry{Int: 16, Float: 1.600000, String: "1.600000", Time: now, Bool: true, Bytes: []byte("1.600000")},
		entry{Int: 17, Float: 1.700000, String: "1.700000", Time: now, Bool: false, Bytes: []byte("1.700000")},
		entry{Int: 18, Float: 1.800000, String: "1.800000", Time: now, Bool: true, Bytes: []byte("1.800000")},
	).Returning("int").QueryRowsPartial(&ints))
	pt.Equal(ints, []int64{15, 16, 17, 18})
}

func (pt *postgresTest) TestUpdate() {
	ds := pt.db.From("entry")
	var e entry
	err := ds.Where(builder.C("int").Eq(9)).Select("id").QueryRow(&e)
	pt.NoError(err)
	e.Int = 11
	_, err = ds.Where(builder.C("id").Eq(e.ID)).Update().Set(e).Exec()
	pt.NoError(err)

	count, err := ds.Where(builder.C("int").Eq(11)).Count()
	pt.NoError(err)
	pt.Equal(int64(1), count)

	var id uint32
	err = ds.Where(builder.C("int").Eq(11)).
		Update().
		Set(builder.Record{"int": 9}).
		Returning("id").QueryRowPartial(&id)
	pt.NoError(err)
	pt.Equal(id, e.ID)
}

func (pt *postgresTest) TestUpdateSQL_multipleTables() {
	ds := pt.db.Update("test")
	updateSQL, _, err := ds.
		Set(builder.Record{"foo": "bar"}).
		From("test_2").
		Where(builder.I("test.id").Eq(builder.I("test_2.test_id"))).
		ToSQL()
	pt.NoError(err)
	pt.Equal(`UPDATE "test" SET "foo"='bar' FROM "test_2" WHERE ("test"."id" = "test_2"."test_id")`, updateSQL)
}

func (pt *postgresTest) TestDelete() {
	ds := pt.db.From("entry")
	var e entry
	err := ds.Where(builder.C("int").Eq(9)).Select("id").QueryRow(&e)
	pt.NoError(err)
	_, err = ds.Where(builder.C("id").Eq(e.ID)).Delete().Exec()
	pt.NoError(err)

	count, err := ds.Count()
	pt.NoError(err)
	pt.Equal(int64(9), count)

	var id uint32
	err = ds.Where(builder.C("id").Eq(e.ID)).QueryRow(&id)
	pt.NoError(err)

	e = entry{}
	err = ds.Where(builder.C("int").Eq(8)).Select("id").QueryRow(&e)
	pt.NoError(err)
	pt.NotEqual(e.ID, int64(0))

	id = 0
	err = ds.Where(builder.C("id").Eq(e.ID)).Delete().Returning("id").QueryRowPartial(&id)
	pt.NoError(err)
	pt.Equal(id, e.ID)
}

func (pt *postgresTest) TestInsert_OnConflict() {
	ds := pt.db.From("entry")
	now := time.Now()

	// DO NOTHING insert
	e := entry{Int: 10, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")}
	_, err := ds.Insert().Rows(e).OnConflict(builder.DoNothing()).Exec()
	pt.NoError(err)

	// DO NOTHING duplicate
	e = entry{Int: 10, Float: 2.100000, String: "2.100000", Time: now.Add(time.Hour * 100), Bool: false, Bytes: []byte("2.100000")}
	_, err = ds.Insert().Rows(e).OnConflict(builder.DoNothing()).Exec()
	pt.NoError(err)

	// DO NOTHING update
	var entryActual entry
	e2 := entry{Int: 0, String: "2.000000"}
	_, err = ds.Insert().
		Rows(e2).
		OnConflict(builder.DoUpdate("int", builder.Record{"string": "upsert"})).
		Exec()
	pt.NoError(err)
	err = ds.Where(builder.C("int").Eq(0)).QueryRow(&entryActual)
	pt.NoError(err)
	pt.Equal("upsert", entryActual.String)

	// DO NOTHING update where
	entries := []entry{
		{Int: 1, Float: 6.100000, String: "6.100000", Time: now, Bytes: []byte("6.100000")},
		{Int: 2, Float: 7.200000, String: "7.200000", Time: now, Bytes: []byte("7.200000")},
	}
	_, err = ds.Insert().
		Rows(entries).
		OnConflict(builder.DoUpdate("int", builder.Record{"string": "upsert"}).Where(builder.I("excluded.int").Eq(2))).
		Exec()
	pt.NoError(err)

	var entry8, entry9 entry
	err = ds.Where(builder.Ex{"int": 1}).QueryRow(&entry8)
	pt.NoError(err)
	pt.Equal("0.100000", entry8.String)

	err = ds.Where(builder.Ex{"int": 2}).QueryRow(&entry9)
	pt.NoError(err)
	pt.Equal("upsert", entry9.String)
}

func (pt *postgresTest) TestWindowFunction() {
	ds := pt.db.From("entry").
		Select("int", builder.ROW_NUMBER().OverName(builder.I("w")).As("id")).
		Window(builder.W("w").OrderBy(builder.I("int").Desc()))

	var entries []entry
	pt.NoError(ds.QueryRows(&entries))

	pt.Equal([]entry{
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
}

func (pt *postgresTest) TestOrderByFunction() {
	ds := pt.db.From("entry").
		Select(builder.ROW_NUMBER().Over(builder.W()).As("id")).Window().Order(builder.ROW_NUMBER().Over(builder.W()).Desc())

	var entries []entry
	pt.NoError(ds.QueryRows(&entries))

	pt.Equal([]entry{
		{ID: 10},
		{ID: 9},
		{ID: 8},
		{ID: 7},
		{ID: 6},
		{ID: 5},
		{ID: 4},
		{ID: 3},
		{ID: 2},
		{ID: 1},
	}, entries)
}

func TestPostgresSuite(t *testing.T) {
	suite.Run(t, new(postgresTest))
}
