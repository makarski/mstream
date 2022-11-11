#### Docker mongo cluster
https://www.mongodb.com/compatibility/deploying-a-mongodb-cluster-with-docker

Update /etc/hosts bind localhost to cluster hostnames

```sh
127.0.0.1 mongo1 mongo2 mongo3
```

#### Running

Install [gcloud](https://cloud.google.com/sdk/docs/install)

```sh
# config your app
$ cp config.toml.dist config.toml

# Spawn mongo cluster in docker
$ make db-up

# Persist sample data
$ make run-persist

# Listen to db events and publish to pubsub
$ make run-listen
```
