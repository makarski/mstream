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
	cargo run -F listen -- $(auth_token)

run-persist:
	cargo run -F persist --no-default-features

run-subscribe:
	cargo run -F subscribe -- $(auth_token)
