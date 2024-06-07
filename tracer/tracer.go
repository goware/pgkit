package tracer

import (
	"context"
	"github.com/jackc/pgx/v5"
)

type ctxKey string

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

func WithTracingEnabled(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, ctxKey("enabled"), enabled)
}

func isTracingEnabled(ctx context.Context) bool {
	enabled, ok := ctx.Value(ctxKey("enabled")).(bool)
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
