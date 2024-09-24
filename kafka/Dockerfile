FROM cnfldemos/cp-server-connect-datagen:0.6.4-7.6.0

RUN confluent-hub install --no-prompt --verbose mongodb/kafka-connect-mongodb:latest

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"
