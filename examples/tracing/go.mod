module github.com/goware/pgkit/v2/example/tracing

go 1.22.3

replace github.com/goware/pgkit/v2 => ../../

replace github.com/goware/pgkit/v2/tracer => ../../tracer

require (
	github.com/goware/pgkit/v2 v2.1.0
	github.com/goware/pgkit/v2/tracer v0.0.0-00010101000000-000000000000
)

require (
	github.com/Masterminds/squirrel v1.5.4 // indirect
	github.com/georgysavva/scany/v2 v2.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20231201235250-de7065d80cb9 // indirect
	github.com/jackc/pgx/v5 v5.6.0 // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	golang.org/x/crypto v0.21.0 // indirect
	golang.org/x/sync v0.6.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)
