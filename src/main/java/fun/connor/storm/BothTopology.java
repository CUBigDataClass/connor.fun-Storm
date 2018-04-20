package fun.connor.storm;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaConfig;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.api.OffsetRequest;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class BothTopology {
    private static final Logger LOG = LoggerFactory.getLogger(BothTopology.class);

    public static void main(String[] args) throws IllegalArgumentException, KeeperException, InterruptedException,
            AlreadyAliveException, InvalidTopologyException, IOException {
        TopologyBuilder rawBuilder = new TopologyBuilder(); // This topology will process raw tweets
        TopologyBuilder aveBuilder = new TopologyBuilder(); // This topology will process sorted tweets

        //TODO: Read zookeeper IP from command line args

        // kafka -> sort_bolt -- topic is 'raw-tweets'
        String topicName = "raw-tweets";
        BrokerHosts hosts = new ZkHosts("35.182.127.62:2181", "/brokers"); // Assumes Kafka broker uses same zk
        // Takes in: BrokerHosts object, topic, zkRoot, zkSpoutID (here random)
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
        spoutConfig.zkPort = 2181;
        spoutConfig.zkServers = Arrays.asList("35.182.127.62");
        spoutConfig.startOffsetTime = OffsetRequest.LatestTime();
        spoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        spoutConfig.fetchSizeBytes = 1024 * 1024 * 4;

        rawBuilder.setSpout("raw_spout", new KafkaSpout(spoutConfig));
        rawBuilder.setBolt("sorting_bolt", new SortBolt(), 20).setNumTasks(20).shuffleGrouping("raw_spout");
        // sort_bolt -> sentiment_bolt
        // sentiment_bolt -> kafka (USE REGION FOR TOPIC)

        // TOPOLOGY SPLIT
        // kafka regions -> averagebolt w/ direct grouping (will need #num region spouts)
//        aveBuilder.setBolt("average_bolt",
//                new AverageBolt().withWindow(BaseWindowedBolt.Duration.minutes(10),
//                        BaseWindowedBolt.Duration.minutes(2)),
        aveBuilder.setBolt("weather_bolt", new WeatherBolt(), 1).shuffleGrouping("average_bolt").setMemoryLoad(768.0);
        // Annnd weather bolt already outputs to kafka! Yay!

        Config rawConf = new Config();
        rawConf.setFallBackOnJavaSerialization(true);
        rawConf.setDebug(true);                 
        rawConf.setNumEventLoggers(5);          // Arbritrary
        rawConf.setNumWorkers(1);               // 1 for debugging
        rawConf.setMessageTimeoutSecs(1200000); // 20 mins
        rawConf.registerEventLogger(org.apache.storm.metric.FileBasedEventLogger.class);
        rawConf.setMaxSpoutPending(5000);

        Config aveConf = new Config();
        aveConf.setFallBackOnJavaSerialization(true);
        aveConf.setDebug(true);                 
        aveConf.setNumEventLoggers(5);          // Arbritrary
        aveConf.setNumWorkers(10);              // ^
        aveConf.registerEventLogger(org.apache.storm.metric.FileBasedEventLogger.class);
        aveConf.setMaxSpoutPending(5000);

        try {
            StormSubmitter.submitTopology("sorting-topology", rawConf, rawBuilder.createTopology());
            //StormSubmitter.submitTopology("average-topology", aveConf, aveBuilder.createTopology());
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }
    }

}
