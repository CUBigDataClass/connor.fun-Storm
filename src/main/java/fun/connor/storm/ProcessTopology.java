package fun.connor.storm;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.stormspout.InitialPositionInStream;
import com.amazonaws.services.kinesis.stormspout.KinesisSpout;
import com.amazonaws.services.kinesis.stormspout.KinesisSpoutConfig;

public class ProcessTopology {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessTopology.class);

    public static void main(String[] args) throws IllegalArgumentException, KeeperException, InterruptedException,
            AlreadyAliveException, InvalidTopologyException, IOException {
        TopologyBuilder rawBuilder = new TopologyBuilder();

        // kafka -> sort_bolt -- ????
        rawBuilder.setBolt("sorting_bolt", new SortBolt(), 2).setNumTasks(2).shuffleGrouping("raw_spout");
        // sort_bolt -> kafka (USE REGION FOR TOPIC) I will do this is sorting_bolt -- Sam

        // TOPOLOGY SPLIT
        TopologyBuilder aveBuilder = new TopologyBuilder();
        // kafka regions -> averagebolt w/ direct grouping (will need #num region spouts)
        aveBuilder.setBolt("average_bolt",
                new AverageBolt().withWindow(BaseWindowedBolt.Duration.minutes(10),
                        BaseWindowedBolt.Duration.minutes(2)),
                10).setNumTasks(10).fieldsGrouping("print_bolt", new Fields("regionID")).setMemoryLoad(768.0);
        aveBuilder.setBolt("weather_bolt", new WeatherBolt(), 1).shuffleGrouping("average_bolt").setMemoryLoad(768.0);
        // Annnd weather bolt already outputs to kafka! Yay!

        Config topoConf = new Config();
        topoConf.setFallBackOnJavaSerialization(true);
        topoConf.setDebug(true);                 
        topoConf.setNumEventLoggers(5);          // Arbritrary
        topoConf.setNumWorkers(10);              // ^
        topoConf.setMessageTimeoutSecs(1200000); // 20 mins
        topoConf.registerEventLogger(org.apache.storm.metric.FileBasedEventLogger.class);

        topoConf.setMaxSpoutPending(5000);
        try {
            StormSubmitter.submitTopology("sorting-topology", topoConf, rawBuilder.createTopology());
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }
    }

}
