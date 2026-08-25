.PHONY: help build test integration-test clean stack-up stack-down seed reset validate run dry-run inspect

JAR := target/warehouse-sync-0.1.0-all.jar
CONFIG ?= sync-configs/users_sync.json

help:
	@echo "warehouse-sync - Make targets"
	@echo ""
	@echo "  build             Build the fat jar"
	@echo "  test              Run unit tests"
	@echo "  integration-test  Run testcontainers integration tests"
	@echo "  clean             Clean build artifacts"
	@echo ""
	@echo "  stack-up          Start shared lakehouse + demo source-postgres"
	@echo "  stack-down        Stop stack (keeps volumes)"
	@echo "  seed              Generate churn in source-postgres"
	@echo "  reset             Nuke stack + volumes, rebuild, restart"
	@echo ""
	@echo "  validate          CONFIG=<path> validate a sync config"
	@echo "  run               CONFIG=<path> run one sync"
	@echo "  dry-run           CONFIG=<path> preview one sync"
	@echo "  inspect           show state store contents"
	@echo ""

build:
	mvn -q -DskipTests package

test:
	mvn -q test

integration-test:
	mvn -q verify -DskipUnitTests

clean:
	mvn -q clean

stack-up:
	./scripts/start-stack.sh

stack-down:
	docker compose -f $${SPARK_MINIO_TRINO_DIR:-../spark_minio_trino}/docker-compose.yml -f docker-compose.yml -f lakehouse-override.yml down

seed:
	./scripts/seed-postgres.sh

reset:
	./scripts/reset-and-rebuild.sh

validate: $(JAR)
	java -jar $(JAR) validate-config --config $(CONFIG)

run: $(JAR)
	java -jar $(JAR) run --config $(CONFIG)

dry-run: $(JAR)
	java -jar $(JAR) dry-run --config $(CONFIG)

inspect: $(JAR)
	java -jar $(JAR) inspect

$(JAR):
	mvn -q -DskipTests package
