.?PHONY: db-up db-stop db-check

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
