package tracer

import (
	"context"
	"time"
)

type config struct {
	logAllQueries           bool
	logFailedQueries        bool
	logValues               bool
	logSlowQueriesThreshold time.Duration
	logStart                func(ctx context.Context, query string, args []any)
	logSlowQuery            func(ctx context.Context, query string, duration time.Duration)
	logEnd                  func(ctx context.Context, query string, duration time.Duration)
	logFailedQuery          func(ctx context.Context, query string, err error)
}

type optionFunc func(config *config)

func (o optionFunc) apply(c *config) {
	o(c)
}

type Option interface {
	apply(*config)
}

func WithLogStart(f func(ctx context.Context, query string, args []any)) Option {
	return optionFunc(func(c *config) {
		c.logStart = f
	})
}

func WithLogSlowQuery(f func(ctx context.Context, query string, duration time.Duration)) Option {
	return optionFunc(func(c *config) {
		c.logSlowQuery = f
	})
}

func WithLogFailedQuery(f func(ctx context.Context, query string, err error)) Option {
	return optionFunc(func(c *config) {
		c.logFailedQuery = f
	})
}

func WithLogEnd(f func(ctx context.Context, query string, duration time.Duration)) Option {
	return optionFunc(func(c *config) {
		c.logEnd = f
	})
}

func WithLogAllQueries() Option {
	return optionFunc(func(config *config) {
		config.logAllQueries = true
	})
}

func WithLogFailedQueries() Option {
	return optionFunc(func(config *config) {
		config.logFailedQueries = true
	})
}

func WithLogValues() Option {
	return optionFunc(func(config *config) {
		config.logValues = true
	})
}

func WithLogSlowQueriesThreshold(threshold time.Duration) Option {
	return optionFunc(func(config *config) {
		config.logSlowQueriesThreshold = threshold
	})
}
