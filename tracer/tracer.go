package tracer

import (
	"context"
	"github.com/jackc/pgx/v5"
)

type ctxKey string

var (
	contextKeyQueryStart     = ctxKey("query_start")
	contextKeyQuery          = ctxKey("query")
	contextKeyTracingEnabled = ctxKey("tracing_enabled")
)

// Tracer
// see: https://github.com/jackc/pgx/blob/master/tracer.go
// Not implemented: CopyFromTracer, PrepareTracer, PrepareTracer ( not needed now )
type Tracer interface {
	pgx.QueryTracer
	pgx.BatchTracer
}

type SQLTracer struct {
	tracers []Tracer
}

func NewSQLTracer(tracers ...Tracer) *SQLTracer {
	return &SQLTracer{tracers: tracers}
}

func WithTracingEnabled(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKeyTracingEnabled, true)
}

func isTracingEnabled(ctx context.Context) bool {
	enabled, ok := ctx.Value(contextKeyTracingEnabled).(bool)
	if !ok {
		return false
	}
	return enabled
}

func (s *SQLTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	for _, tracer := range s.tracers {
		ctx = tracer.TraceQueryStart(ctx, conn, data)
	}

	return ctx
}

func (s *SQLTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	for _, tracer := range s.tracers {
		tracer.TraceQueryEnd(ctx, conn, data)
	}
}

func (s *SQLTracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	for _, tracer := range s.tracers {
		ctx = tracer.TraceBatchStart(ctx, conn, data)
	}

	return ctx
}

func (s *SQLTracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	for _, tracer := range s.tracers {
		tracer.TraceBatchQuery(ctx, conn, data)
	}
}

func (s *SQLTracer) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
	for _, tracer := range s.tracers {
		tracer.TraceBatchEnd(ctx, conn, data)
	}
}
