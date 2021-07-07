SHELL            = bash -o pipefail
TEST_FLAGS       ?= -p 1 -v
# MOD_VENDOR       ?= -mod=vendor

PG_HOST           ?= 127.0.0.1
PG_PASSWORD       ?= postgres
PG_USER           ?= postgres
PG_DATABASE       ?= pgkit_test


all:
	@echo "make <cmd>"
	@echo ""
	@echo "commands:"
	@echo ""
	@echo " + Development:"
	@echo "   - build"
	@echo "   - test"
	@echo "   - todo"
	@echo "   - clean"
	@echo ""
	@echo ""
	@echo " + Database stuff:"
	@echo "   - db-reset"
	@echo "   - db-create"
	@echo "   - db-drop"
	@echo ""


##
## Development
##
build:
	go build ./...

clean:
	go clean -cache -testcache

test: test-clean
	GOGC=off go test $(TEST_FLAGS) $(MOD_VENDOR) -run=$(TEST) .

test-all: test-clean
	GOGC=off go test $(TEST_FLAGS) $(MOD_VENDOR) -run=$(TEST) ./...

test-with-reset: db-reset test-all

test-clean:
	GOGC=off go clean -testcache

todo:
	@git grep TODO -- './*' ':!./vendor/' ':!./Makefile' || :


##
## Database stuff
##
db-reset:
	@($(MAKE) db-drop || exit 0) && \
	($(MAKE) db-create || exit 0) && \
	PGPASSWORD=$(PG_PASSWORD) PG_USER=$(PG_USER) PG_HOST=$(PG_HOST) ./scripts/db.sh import $(PG_DATABASE) ./testdata/pgkit_test_db.sql

db-create:
	@env PG_USER=$(PG_USER) ./scripts/db.sh create $(PG_DATABASE)

db-drop:
	@env PG_USER=$(PG_USER) ./scripts/db.sh drop $(PG_DATABASE)
