.?PHONY: db-up db-stop db-check auth run-listen print-token integration-tests unit-tests

auth_token=$(shell gcloud auth print-access-token)

db-up:
	@docker-compose up -d

db-stop:
	@docker-compose stop

db-check:
	@docker exec -it mongo1 mongosh --eval "rs.status()"

auth:
	gcloud auth login

run-listen:
	RUST_LOG=info cargo run -- $(auth_token)

print-token:
	gcloud auth print-access-token

integration-tests:
	RUST_LOG=info AUTH_TOKEN=$(auth_token) cargo test -- --nocapture --ignored

unit-tests:
	RUST_LOG=info cargo test -- --nocapture
