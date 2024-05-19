AUTH_TOKEN=$(shell gcloud auth print-access-token)
PRIMARY_HOST := $(shell docker exec -it mongo1 mongosh --eval "rs.status().members.find(member => member.stateStr === 'PRIMARY').name" | grep -E 'mongo[1-3]:27017' | awk -F ':' '{print $$1}')

.PHONY: help
help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: print-primary-host
print-primary-host: ## Prints the primary mongo host
	@echo $(PRIMARY_HOST)

.PHONY: up
up: db-up db-init-rpl-set ## Starts the mongo cluster and initializes the replica set

.PHONY: db-up
db-up: ## Starts the mongo cluster
	@docker-compose up -d

.PHONY: db-init-rpl-set
db-init-rpl-set: ## Initializes the replica set
	@docker exec mongo1 /bin/bash /opt/scripts/init_replica_set.sh

.PHONY: db-stop
db-stop: ## Stops the mongo cluster
	@docker-compose stop

.PHONY: db-check
db-check: ## Checks the status of the mongo cluster
	@docker exec -it mongo1 mongosh --eval "rs.status()"

.PHONY: auth
auth: ## Authenticates with gcloud
	gcloud auth login

.PHONY: setup-config
setup-config: ## Sets up the config file
	@./setup_config.sh

.PHONY: run-debug
run-debug: setup-config ## Runs the server in debug mode
	RUST_LOG=debug cargo run

.PHONY: print-token
print-token: ## Prints the access token
	@echo $(AUTH_TOKEN)

.PHONY: db-fixtures
db-fixtures: ## Loads the fixtures into the db
	@db_name=employees; \
    read -p "Enter db name (default is $$db_name): " user_db_name; \
    if [ -n "$$user_db_name" ]; then \
        db_name=$$user_db_name; \
    fi; \
    docker exec $(PRIMARY_HOST) mongoimport --db $$db_name --collection employees --file /opt/fixtures/fixtures.json --jsonArray

.PHONY: integration-tests
integration-tests: ## Runs the integration tests
	@source .env.test && RUST_LOG=debug \
	 AUTH_TOKEN=$(AUTH_TOKEN) \
	 cargo test -- --nocapture --ignored

.PHONY: unit-tests
unit-tests: ## Runs the unit tests
	RUST_LOG=info cargo test -- --nocapture
