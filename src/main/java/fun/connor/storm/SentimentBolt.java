package fun.connor.storm;

import java.util.Map;

import org.apache.storm.tuple.Fields;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;

public class SentimentBolt extends ShellBolt implements IRichBolt {
    private static final long serialVersionUID = 177788290277633253L;

    public SentimentBolt() {
        super("python3", "SentimentBolt.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("regionID", "sentiment", "tweetID", "regionJSON", "possiblySensitive"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


}
