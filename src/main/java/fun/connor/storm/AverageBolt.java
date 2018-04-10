package fun.connor.storm;

import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AverageBolt extends BaseWindowedBolt{
    private final Logger LOG = LoggerFactory.getLogger(AverageBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector){
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow){
        double sentiment_sum = 0;
        List<Tuple> tuplesInWindow = inputWindow.get();
        if (tuplesInWindow.size() > 0){
            // Keep track of an average
            float sumSentiment = 0;
            for (Tuple tuple: tuplesInWindow){
                // Get fields and pull out sentiment
                // Add sentiment to sum
                sumSentiment += (float) tuple.getValue(1);
            }

            float avgSentiment = sumSentiment / tuplesInWindow.size();
            String regionID = (String) tuplesInWindow.get(1).getValue(0);
            String avgTweetID = (String) tuplesInWindow.get(1).getValue(2);
            Object regionJSON = tuplesInWindow.get(1).getValue(3);

            collector.emit(new Values(regionID, avgSentiment, avgTweetID, regionJSON));
            // Output the data: region average, region ID, and typical tweet for the window.
            LOG.info("AverageBolt got region: regionID=" + tuplesInWindow.get(1).getValue(0) + " with average sentiment of " + avgSentiment);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("regionID", "avgSentiment", "exemplarTweetID", "regionJSON"));
    }

}
