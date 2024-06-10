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
	logStartHook            func(ctx context.Context, query string, args []any)
	logSlowQueryHook        func(ctx context.Context, query string, duration time.Duration)
	logEndQueryHook         func(ctx context.Context, query string, duration time.Duration)
	logFailedQueryHook      func(ctx context.Context, query string, err error)
}

type optionFunc func(config *config)

func (o optionFunc) apply(c *config) {
	o(c)
}

type Option interface {
	apply(*config)
}

func WithLogStartHook(f func(ctx context.Context, query string, args []any)) Option {
	return optionFunc(func(c *config) {
		c.logStartHook = f
	})
}

func WithLogSlowQueryHook(f func(ctx context.Context, query string, duration time.Duration)) Option {
	return optionFunc(func(c *config) {
		c.logSlowQueryHook = f
	})
}

func WithLogFailedQueryHook(f func(ctx context.Context, query string, err error)) Option {
	return optionFunc(func(c *config) {
		c.logFailedQueryHook = f
	})
}

func WithLogEndHook(f func(ctx context.Context, query string, duration time.Duration)) Option {
	return optionFunc(func(c *config) {
		c.logEndQueryHook = f
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
