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
	RUST_LOG=info cargo run -F listen -- $(auth_token)

run-persist:
	RUST_LOG=info cargo run -F persist --no-default-features

run-subscribe:
	RUST_LOG=info cargo run -F subscribe --no-default-features -- $(auth_token) 
