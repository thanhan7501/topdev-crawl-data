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

