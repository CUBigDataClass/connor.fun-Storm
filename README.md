# connor.fun-Storm
Files for creating storm clusters and running analysis on incoming stream data from Kinesis.

#### Running - In current config
A couple things need to be set: In `sample.properties`, `zookeeperEndpoint` should be set to the correct IP/domain.

Similarly, `nimbus.seeds` in `/path/to/storm/conf/storm.yaml` needs to be set to the correct IP/domain of the nimbus server.

Finally, we need to install an updated version of the storm-kinesis-spout: the jar is in lib, to add it to maven run:
```
mvn install:install-file -Dfile=lib/kinesis-storm-spout-1.1.2-SNAPSHOT.jar -DgroupId=com.amazonaws.services.kinesis.stormspout -DartifactId=kinesis-storm-spout -Dversion=1.1.2-SNAPSHOT -Dpackaging=jar
```

For actually running a topology on a remote cluster:
```
$ mvn package
$ /path/to/storm/bin/storm jar target/storm-1.0.jar fun.connor.storm.ProcessTopology ./sample.properties RemoteMode
```

Running locally is also possible:
```
$ /path/to/storm/bin/storm jar target/storm-1.0.jar fun.connor.storm.ProcessTopology ./sample.properties LocalMode
```
