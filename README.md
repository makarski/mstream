#### Docker mongo cluster
https://www.mongodb.com/compatibility/deploying-a-mongodb-cluster-with-docker


#### Running

Install [gcloud](https://cloud.google.com/sdk/docs/install)

```sh
# config your app
$ cp config.toml.dist config.toml

# Spawn mongo cluster in docker
$ make db-up
$ make db-check

# Listen to db events and publish to pubsub
$ make run-listen
```
