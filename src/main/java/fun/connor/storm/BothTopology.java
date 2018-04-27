package fun.connor.storm;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
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
  private static String zookeeperEndpoint;
  private static String webserverEndpoint;
  private static String zookeeperPrefix;

  public static void main(String[] args)
      throws IllegalArgumentException, KeeperException, InterruptedException,
             AlreadyAliveException, InvalidTopologyException, IOException {

    String propertiesFile = null;

    if (args.length != 1) {
      printUsageAndExit();
    } else {
      propertiesFile = args[0];
    }

    configure(propertiesFile);

    TopologyBuilder rawBuilder = new TopologyBuilder();

    String topicName = "raw-tweets-USA0";
    BrokerHosts hosts = new ZkHosts(
        zookeeperEndpoint, "/brokers"); // Assumes Kafka broker uses same zk
    SpoutConfig spoutConfig = new SpoutConfig(
        hosts, topicName, "/" + zookeeperPrefix, UUID.randomUUID().toString());
    spoutConfig.startOffsetTime = OffsetRequest.LatestTime();
    rawBuilder.setSpout("raw_spout1", new KafkaSpout(spoutConfig));

    topicName = "raw-tweets-USA1";
    spoutConfig = new SpoutConfig(
        hosts, topicName, "/" + zookeeperPrefix, UUID.randomUUID().toString());
    spoutConfig.startOffsetTime = OffsetRequest.LatestTime();
    rawBuilder.setSpout("raw_spout2", new KafkaSpout(spoutConfig));

    rawBuilder.setBolt("sorting_bolt", new SortBolt(webserverEndpoint), 3)
        .shuffleGrouping("raw_spout1").shuffleGrouping("raw_spout2");
    rawBuilder.setBolt("sentiment_bolt", new SentimentBolt(), 40)
        .shuffleGrouping("sorting_bolt");

    rawBuilder
        .setBolt(
            "average_bolt",
            new AverageBolt().withWindow(BaseWindowedBolt.Duration.minutes(10),
                                         BaseWindowedBolt.Duration.minutes(2)),
            10)
        .fieldsGrouping("sentiment_bolt", new Fields("regionID")).setMemoryLoad(798.0);
    rawBuilder.setBolt("weather_bolt", new WeatherBolt(), 2)
        .shuffleGrouping("average_bolt")
        .setMemoryLoad(768.0);

    Config rawConf = new Config();
    rawConf.setFallBackOnJavaSerialization(true);
    rawConf.setDebug(false);
    rawConf.setNumEventLoggers(1);       // Arbritrary
    rawConf.setNumWorkers(40);           // ^
    rawConf.setMessageTimeoutSecs(1400); // 22 mins
    rawConf.registerEventLogger(org.apache.storm.metric.FileBasedEventLogger.class);
    rawConf.setMaxSpoutPending(5000);

    // I have no idea how to fix the spout lag problem - the bottleneck is nowhere I can see.
    // rawConf.put("topology.producer.batch.size", 262144);
    // rawConf.put("topology.transfer.buffer.size", 262144);
    // rawConf.put("topology.executor.receive.buffer.size", 262144);
    // rawConf.put("topology.worker.max.heap.size.mb", 2000);
    // rawConf.put("topology.sleep.spout.wait.strategy.time.ms", 0);

    try {
      StormSubmitter.submitTopology("the-topology", rawConf,
                                    rawBuilder.createTopology());
    } catch (AuthorizationException e) {
      e.printStackTrace();
    }
  }

  private static void configure(String propertiesFile) throws IOException {
    FileInputStream inputStream = new FileInputStream(propertiesFile);
    Properties properties = new Properties();
    try {
      properties.load(inputStream);
    } finally {
      inputStream.close();
    }

    String zookeeperEndpointOverride =
        properties.getProperty("zookeeperEndpoint");
    if (zookeeperEndpointOverride != null) {
      zookeeperEndpoint = zookeeperEndpointOverride;
    }
    LOG.info("Using zookeeper endpoint " + zookeeperEndpoint);

    String zookeeperPrefixOverride = properties.getProperty("zookeeperPrefix");
    if (zookeeperPrefixOverride != null) {
      zookeeperPrefix = zookeeperPrefixOverride;
    }
    LOG.info("Using zookeeper prefix " + zookeeperPrefix);

    String webserverEndpointOverride =
        properties.getProperty("webserverEndpoint");
    if (webserverEndpointOverride != null) {
      webserverEndpoint = webserverEndpointOverride;
    }
    LOG.info("Using webserver endpoint " + webserverEndpoint);
  }

  private static void printUsageAndExit() {
    System.out.println("Usage: " + BothTopology.class.getName() +
                       " <propertiesFile>");
    System.exit(-1);
  }
}
