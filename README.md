# connor.fun-Storm
Files for creating storm topologies and running analysis on incoming stream data.

#### Running - In current config
A couple things need to be set: In `sample.properties`, `zookeeperEndpoint` should be set to the correct IP/domain.

Similarly, `nimbus.seeds` and `storm.zookeeper.servers` in `/path/to/storm/conf/storm.yaml` needs to be set to the correct IP/domain of the Nimbus/Zookeeper server.

For actually running a topology on a cluster:
```
$ mvn package
$ /path/to/storm/bin/storm jar target/storm-1.0.jar fun.connor.storm.BothTopology ./sample.properties
```

