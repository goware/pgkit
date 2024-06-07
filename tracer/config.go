package tracer

import "time"

type config struct {
	logAllQueries           bool
	logFailedQueries        bool
	logValues               bool
	logSlowQueriesThreshold time.Duration
}

type optionFunc func(config *config)

func (o optionFunc) apply(c *config) {
	o(c)
}

type Option interface {
	apply(*config)
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
