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

public class AverageBolt extends BaseWindowedBolt {
    private static final long serialVersionUID = 177788294989633253L;
    private final Logger LOG = LoggerFactory.getLogger(AverageBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        
        List<Tuple> tuplesInWindow = inputWindow.get();
        if (tuplesInWindow.size() > 0) {
            // Keep track of an average
            float sumSentiment = 0;
            int counter = 0;
            float tempAverage = 0;

            String regionID = (String) tuplesInWindow.get(0).getValue(0);
            String avgTweetID = (String) tuplesInWindow.get(0).getValue(2);
            Object regionJSON = tuplesInWindow.get(0).getValue(3);
            Double avgTweetSent = (Double) tuplesInWindow.get(0).getValue(1);

            for (Tuple tuple : tuplesInWindow) {

                // Get fields and pull out sentiment
                // Add sentiment to sum
                // Log if tweet doesn't have the correct region ID
                if (!tuple.getValue(0).equals(regionID)) {
                    LOG.info("Tweet with ID " + tuple.getValue(2) + " does not correspond with expected region "
                            + regionID + ", has ID " + tuple.getValue(0));
                } else { // include in average?
                    sumSentiment += (Double) tuple.getValue(1);
                    tempAverage = sumSentiment / counter;
                    if (((Double) tuple.getValue(1) - tempAverage) < 0.001) {
                        avgTweetID = (String) tuple.getValue(2);
                        avgTweetSent = (Double) tuple.getValue(1);
                    }
                    counter++;
                }
            }

            float avgSentiment = sumSentiment / tuplesInWindow.size();
            //LOG.info("AverageBolt got indicative tweet " + avgTweetID + " with sentiment " + avgTweetSent);
            collector.emit(new Values(regionID, avgSentiment, avgTweetID, regionJSON));
            // Output the data: region average, region ID, and typical tweet for the window.
            //LOG.info("AverageBolt got region: regionID=" + tuplesInWindow.get(0).getValue(0)
            //        + " with average sentiment of " + avgSentiment);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("regionID", "avgSentiment", "exemplarTweetID", "regionJSON"));
    }

}
