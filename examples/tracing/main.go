package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/goware/pgkit/v2"
	"github.com/goware/pgkit/v2/tracer"
)

func main() {
	handler := slog.NewJSONHandler(os.Stderr, nil)
	logger := slog.New(handler)

	conf := pgkit.Config{
		Database:        "pgkit_test",
		Host:            "localhost",
		Username:        "postgres",
		Password:        "postgres",
		ConnMaxLifetime: "1h",
		Tracer:          tracer.NewSlogTracer(logger, tracer.WithLogAllQueries(), tracer.WithLogValues(), tracer.WithLogFailedQueries()),
	}

	dbClient, err := pgkit.Connect("pgkit_test", conf)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to connect dbClient: %w", err))
	}

	defer dbClient.Conn.Close()

	err = dbClient.Conn.Ping(context.Background())
	if err != nil {
		log.Fatal(fmt.Errorf("failed to ping dbClient: %w", err))
	}

	// successful query
	stmt := pgkit.RawQuery("SELECT * FROM accounts WHERE name IN (?,?)")
	q := stmt.Build("user-1", "user-2")

	_, err = dbClient.Query.Exec(context.Background(), q)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to execute query: %w", err))
	}

	// example with incorrect sql query
	stmt = pgkit.RawQuery("SELECT * FROM non_existent_table")
	_, _ = dbClient.Query.Exec(context.Background(), stmt.Build())
}
