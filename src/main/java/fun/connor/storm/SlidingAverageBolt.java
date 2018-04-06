package fun.connor.storm;

import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.starter.bolt.SlidingWindowSumBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public static class SlidingAverageBolt extends BaseWindowedBolt{
    private OutputCollector collector;
    
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector){
      this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow){
      double sentiment_sum = 0;
      List<Tuple> tuplesInWindow = inputWindow.get();
      if (tuplesInWindow.size() > 0){
        for (Tuple tuple: tuplesInWindow){
          // Get fields and pull out sentiment
          // Add sentiment to sum
          LOG.
        }
        collector.emit() // Output the data: region average, region ID, and typical tweet for the window.
      }
    }

}
