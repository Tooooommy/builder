package builder_test

import (
	"context"
	"fmt"
	"time"

	"github.com/Tooooommy/builder/v9"
)

func ExampleDatabase_Begin() {
	db := getDB()

	db.Transact(func(td *builder.TxDatabase) error {
		// use tx.From to get a dataset that will execute within this transaction
		ret, err := td.Update("builder_user").
			Set(builder.Record{"last_name": "Ucon"}).
			Where(builder.Ex{"last_name": "Yukon"}).Exec()
		if err != nil {
			return err
		}
		affect, err := ret.RowsAffected()
		if err != nil {
			return err
		}
		fmt.Printf("Updated users in transaction affected %+v\n", affect)
		return nil
	})

	// Output:
	// Updated users in transaction affected 3
}

func ExampleDatabase_BeginTx() {
	db := getDB()

	ctx := context.Background()
	db.TransactCtx(ctx, func(ctx context.Context, td *builder.TxDatabase) error {
		// use tx.From to get a dataset that will execute within this transaction
		ret, err := td.Update("builder_user").
			Set(builder.Record{"last_name": "Ucon"}).
			Where(builder.Ex{"last_name": "Yukon"}).Exec()
		if err != nil {
			return err
		}
		affect, err := ret.RowsAffected()
		if err != nil {
			return err
		}
		fmt.Printf("Updated users in transaction affect %+v", affect)
		return nil
	})

	// Output:
	// Updated users in transaction affect 3
}

func ExampleDatabase_WithTx() {
	db := getDB()

	db.Transact(func(td *builder.TxDatabase) error {
		// use tx.From to get a dataset that will execute within this transaction
		ret, err := td.Update("builder_user").
			Where(builder.Ex{"last_name": "Yukon"}).
			Set(builder.Record{"last_name": "Ucon"}).
			Exec()
		if err != nil {
			return err
		}
		affect, err := ret.RowsAffected()
		if err != nil {
			return err
		}
		fmt.Printf("Updated users in transaction affect %+v", affect)
		return nil
	})

	// Output:
	// Updated users in transaction affect 3
}

func ExampleDatabase_Dialect() {
	db := getDB()

	fmt.Println(db.Dialect())

	// Output:
	// mysql
}

func ExampleDatabase_Exec() {
	db := getDB()

	_, err := db.Exec(`DROP TABLE user_role`)
	if err != nil {
		fmt.Println("Error occurred while dropping tables", err.Error())
	}
	_, err = db.Exec(`DROP TABLE builder_user`)
	if err != nil {
		fmt.Println("Error occurred while dropping tables", err.Error())
	}
	fmt.Println("Dropped tables user_role and builder_user")

	// Output:
	// Dropped tables user_role and builder_user
}

func ExampleDatabase_ExecContext() {
	db := getDB()
	d := time.Now().Add(time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	_, err := db.ExecCtx(ctx, `DROP TABLE user_role`)
	if err != nil {
		fmt.Println("Error occurred while dropping tables", err.Error())
	}

	_, err = db.ExecCtx(ctx, `DROP TABLE builder_user`)
	if err != nil {
		fmt.Println("Error occurred while dropping tables", err.Error())
	}

	fmt.Println("Dropped tables user_role and builder_user")
	// Output:
	// Dropped tables user_role and builder_user
}

func ExampleDatabase_From() {
	db := getDB()
	var names []string

	if err := db.From("builder_user").Select("first_name").QueryRows(&names); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Fetched Users names:", names)
	}
	// Output:
	// Fetched Users names: [Bob Sally Vinita John]
}
