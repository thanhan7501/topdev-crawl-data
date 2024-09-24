# Create replica set for MongoDB:

```shell
docker run -d --rm -p 27017:27017 --name some-mongo --network airflow_airflow mongo:latest mongod --replSet myReplicaSet --bind_ip localhost,some-mongo
```

```shell
docker run -d --rm -p 27018:27017 --name mongo-2 --network airflow_airflow mongo:latest mongod --replSet myReplicaSet --bind_ip localhost,mongo-2
```

```shell
docker run -d --rm -p 27019:27017 --name mongo-3 --network airflow_airflow mongo:latest mongod --replSet myReplicaSet --bind_ip localhost,mongo-3
```

```shell
docker exec -it some-mongo mongosh --eval "rs.initiate({
 _id: \"myReplicaSet\",
 members: [
   {_id: 0, host: \"some-mongo"},
   {_id: 1, host: \"mongo-2\"},
   {_id: 2, host: \"mongo-3\"}
 ]
})"
```

# Create connection Kafka - MongoDB

```shell
curl -X POST \
     -H "Content-Type: application/json" \
     --data '{
  "name": "mongo-source",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
    "connection.uri": "mongodb://some-mongo:27017/?replicaSet=myReplicaSet",
    "database": "topdev",
    "collection": "jobs",
    "pipeline": "[{\"$match\": {\"operationType\": \"insert\"}}]",
    "publish.full.document.only": "true"
  }
}' \
  http://connect:8083/connectors -w "\n"

```
