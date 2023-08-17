# Log analytics system

## Setup

### Install Kafka

- Change directory to kafka folder
- Setup environment variables `export DOCKER_HOST_IP=x.x.x.x`
- Run `docker compose -f zk-single-kafka-single.yml up -d`
- Go to Kafka machine `docker exec -it <kafka container name> /bin/bash`
- Create topic `kafka-topics --create --topic <topicnamw> --bootstrap-server localhost:19092`
- Consume topic `kafka-console-consumer --topic <topicname> --from-beginning --bootstrap-server localhost:19092`


### Install Filebeat

- Guide: https://www.elastic.co/downloads/beats/filebeat-oss
- Change config file at: `/etc/filebeat/filebeat.yml`
- `filebeat.yml`

```yml
filebeat.inputs:
- type: filestream
  id: access-logs
  paths:
    - "/var/log/apache2/*.log"

output.kafka:
  hosts: ["192.168.193.254:9092"]
  topic: "apache2"
  topics:
    - topic: "error"
      when.contains:
        message: "ERR"
    - topic: "access"
      when contains:
        message: "\"GET .*\" 200"
```
- Test config `filebeat test config`
- Test output `filebeat test output`
  
### Generate fake apache log

```bash
pip install -r requirements.txt
python apache-fake-log-gen.py -n 0 --sleep 1 | tee access-logs/access-logs_$(date +%s).log
```

### Query data from cassandra

```bash
cqlsh 192.168.193.254 9042 -u cassandra -p cassandra
```

---
Reference:
- https://towardsdatascience.com/a-fast-look-at-spark-structured-streaming-kafka-f0ff64107325
- https://zenodo.org/record/8196385
- https://github.com/big-data-europe/docker-hadoop/blob/master/docker-compose.yml