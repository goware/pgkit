package pgkit_test

import (
	"context"
	"fmt"
	"log"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/goware/pgkit"
	"github.com/stretchr/testify/assert"
)

var (
	DB *pgkit.Database
)

func init() {
	mrand.Seed(time.Now().UnixNano())

	var err error
	DB, err = pgkit.Connect("pgkit_test", pgkit.Config{
		Database: "pgkit_test",
		Hosts:    []string{"localhost"},
		Username: "postgres",
		Password: "postgres",
	})
	if err != nil {
		log.Fatal(fmt.Errorf("failed to connect db: %w", err))
	}

	err = DB.Ping(context.Background())
	if err != nil {
		log.Fatal(fmt.Errorf("failed to ping db: %w", err))
	}
}

func TestHi(t *testing.T) {
	assert.True(t, true)
}
