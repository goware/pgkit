package tracer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type LogTracer struct {
	Logger           *slog.Logger
	LogAllQueries    bool
	LogFailedQueries bool
	// replace placeholders with arguments useful for local debugging
	LogValues bool
	// enabled if non-zero value is provided
	LogSlowQueriesThreshold time.Duration

	// give client power to change each section which is being logged
	StartQueryHook  func(ctx context.Context, query string, args []any)
	SlowQueryHook   func(ctx context.Context, query string, duration time.Duration)
	EndQueryHook    func(ctx context.Context, query string, duration time.Duration)
	FailedQueryHook func(ctx context.Context, query string, err error)
}

func NewLogTracer(logger *slog.Logger, opts ...Option) *LogTracer {
	logStart := func(ctx context.Context, query string, args []any) {
		if logger != nil {
			logger.LogAttrs(ctx, slog.LevelInfo, "query start", slog.String("query", query), slog.Any("args", args))
		}
	}

	logSlowQuery := func(ctx context.Context, query string, duration time.Duration) {
		if logger != nil {
			logger.LogAttrs(ctx, slog.LevelWarn, "slow query took", slog.Any("query", query), slog.String("duration", duration.String()))
		}
	}

	logEnd := func(ctx context.Context, query string, duration time.Duration) {
		if logger != nil {
			logger.LogAttrs(ctx, slog.LevelInfo, "query end", slog.Any("query", query), slog.String("duration", duration.String()))
		}
	}

	logFailed := func(ctx context.Context, query string, err error) {
		if logger != nil {
			logger.LogAttrs(ctx, slog.LevelError, "query failed", slog.Any("query", query), slog.String("err", err.Error()))
		}
	}

	cfg := &config{
		logAllQueries:           false,
		logFailedQueries:        false,
		logValues:               false,
		logSlowQueriesThreshold: 0,
		logStartHook:            logStart,
		logSlowQueryHook:        logSlowQuery,
		logEndQueryHook:         logEnd,
		logFailedQueryHook:      logFailed,
	}

	for _, opt := range opts {
		opt.apply(cfg)
	}

	return &LogTracer{
		LogAllQueries:           cfg.logAllQueries,
		LogFailedQueries:        cfg.logFailedQueries,
		LogValues:               cfg.logValues,
		LogSlowQueriesThreshold: cfg.logSlowQueriesThreshold,
		StartQueryHook:          cfg.logStartHook,
		SlowQueryHook:           cfg.logSlowQueryHook,
		EndQueryHook:            cfg.logEndQueryHook,
		FailedQueryHook:         cfg.logFailedQueryHook,
	}
}

func (l *LogTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	query := data.SQL
	if l.LogValues {
		for i, placeholder := range data.Args {
			query = strings.Replace(query, fmt.Sprintf("$%d", i+1), fmt.Sprintf("%v", placeholder), 1)
		}
	}

	if l.LogAllQueries || isTracingEnabled(ctx) {
		l.StartQueryHook(ctx, query, data.Args)
	}

	ctx = context.WithValue(ctx, contextKeyQueryStart, time.Now())
	ctx = context.WithValue(ctx, contextKeyQuery, query)

	return ctx
}

func (l *LogTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	queryStart := getCtxQueryStart(ctx)
	query := getCtxQuery(ctx)
	queryDuration := time.Since(queryStart)

	if l.LogSlowQueriesThreshold > 0 {
		if queryDuration > l.LogSlowQueriesThreshold {
			l.SlowQueryHook(ctx, query, queryDuration)
		}
	}

	if (l.LogAllQueries || isTracingEnabled(ctx)) && data.Err == nil {
		l.EndQueryHook(ctx, query, queryDuration)
	}

	if l.LogFailedQueries && data.Err != nil && !errors.Is(data.Err, sql.ErrNoRows) {
		l.FailedQueryHook(ctx, query, data.Err)
	}
}

func (l *LogTracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	query := data.Batch.QueuedQueries[0]

	return l.TraceQueryStart(ctx, conn, pgx.TraceQueryStartData{
		SQL:  query.SQL,
		Args: query.Arguments,
	})
}

func (l *LogTracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	// do nothing
}

func (l *LogTracer) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
	l.TraceQueryEnd(ctx, conn, pgx.TraceQueryEndData{
		CommandTag: pgconn.CommandTag{},
		Err:        data.Err,
	})
}

func getCtxQuery(ctx context.Context) string {
	query, ok := ctx.Value(ctxKey("query")).(string)
	if !ok {
		return ""
	}

	return query
}

func getCtxQueryStart(ctx context.Context) time.Time {
	queryStart, ok := ctx.Value(contextKeyQueryStart).(time.Time)
	if !ok {
		return time.Time{}
	}

	return queryStart
}
