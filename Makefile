.?PHONY: db-up db-stop db-check auth run-listen print-token integration-tests unit-tests setup-config

auth_token=$(shell gcloud auth print-access-token)

db-up:
	@docker-compose up -d

db-stop:
	@docker-compose stop

db-check:
	@docker exec -it mongo1 mongosh --eval "rs.status()"

auth:
	gcloud auth login

setup-config:
	@./setup_config.sh

run-listen: setup-config
	RUST_LOG=debug cargo run

print-token:
	gcloud auth print-access-token

integration-tests:
	@RUST_LOG=debug AUTH_TOKEN=$(auth_token) cargo test -- --nocapture --ignored

unit-tests:
	RUST_LOG=info cargo test -- --nocapture
