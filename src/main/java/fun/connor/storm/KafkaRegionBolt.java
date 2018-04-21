package fun.connor.storm;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;

public class KafkaRegionBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 177788294567633253L;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaRegionBolt.class);
    private KafkaProducer<String, String> kafkaProducer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.kafkaProducer = new KafkaProducer<>(props);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // doesn't block! tnx kpl, hope this works
        this.kafkaProducer.send(new ProducerRecord<String, String>("sentiment-tweets", input.getString(0), input.getString(1)));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("regionID", "sentiment", "tweetID", "regionJSON"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


}
