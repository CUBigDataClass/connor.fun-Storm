package fun.connor.storm;

import java.io.IOException;

import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        // kafka -> sort_bolt -- topic is 'raw-tweets'
        rawBuilder.setBolt("sorting_bolt", new SortBolt(), 20).setNumTasks(20).shuffleGrouping("raw_spout");
        // sort_bolt -> sentiment_bolt
        // sentiment_bolt -> kafka (USE REGION FOR TOPIC) I will do this is in sentiment_bolt -- Sam

        // TOPOLOGY SPLIT
        // kafka regions -> averagebolt w/ direct grouping (will need #num region spouts)
        aveBuilder.setBolt("average_bolt",
                new AverageBolt().withWindow(BaseWindowedBolt.Duration.minutes(10),
                        BaseWindowedBolt.Duration.minutes(2)),
                600).fieldsGrouping("print_bolt", new Fields("regionID")).setMemoryLoad(768.0);
        aveBuilder.setBolt("weather_bolt", new WeatherBolt(), 1).shuffleGrouping("average_bolt").setMemoryLoad(768.0);
        // Annnd weather bolt already outputs to kafka! Yay!

        Config rawConf = new Config();
        rawConf.setFallBackOnJavaSerialization(true);
        rawConf.setDebug(true);                 
        rawConf.setNumEventLoggers(5);          // Arbritrary
        rawConf.setNumWorkers(10);              // ^
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
