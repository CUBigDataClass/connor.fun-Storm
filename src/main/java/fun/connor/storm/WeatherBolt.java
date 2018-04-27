package fun.connor.storm;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;

public class WeatherBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 177717644989633253L;
    private static final Logger LOG = LoggerFactory.getLogger(WeatherBolt.class);
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
        // Open up weather to pre-prescribed regionID
        JSONObject regionJSON = (JSONObject) input.getValue(3);

        Double lat = this.jsonToDouble(regionJSON.get("centerLat"));
        Double lon = this.jsonToDouble(regionJSON.get("centerLon"));

        // Call the weather API
        String key = System.getenv("WEATHER_KEY");
        String url = String.format("https://api.openweathermap.org/data/2.5/weather?lon=%f&lat=%f&units=imperial&APPID=%s",
                lon, lat, key);

        String regionID = (String) input.getValue(0);
        Double avgSentiment = (Double) input.getValue(1);
        String tweetID = (String) input.getValue(2);

        try {
            InputStream is = new URL(url).openStream();
            try {
                BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
                String weatherJSON = readAll(rd);

                //LOG.info("WeatherBolt got region: regionID=" + regionID + " with average sentiment of " + avgSentiment + " and weather of " + weatherJSON);

                // doesn't block! tnx kpl, hope this works
                this.kafkaProducer.send(new ProducerRecord<String, String>("test", regionID, this.formatOutput(regionID, avgSentiment, tweetID, regionJSON, weatherJSON)));

                // Output fields
                //collector.emit(output);
            } finally {
                is.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String formatOutput(String regionID, Double avgSentiment, String tweetID, JSONObject regionJSON, String weatherJSON) {
        // Parse regionJSON and weatherJSON so they become sub-objects
        Object weatherRaw = null;
        JSONParser parser = new JSONParser();
        try {
            weatherRaw = parser.parse(weatherJSON);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        JSONObject region = regionJSON;
        JSONObject weather = (JSONObject) weatherRaw;
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("sentiment", avgSentiment);
        jsonObject.put("ID", regionID);
        jsonObject.put("tid", tweetID);
        jsonObject.put("region", region);
        jsonObject.put("weather", weather);

        return jsonObject.toJSONString();
    }

    private static String readAll(Reader rd) throws IOException {
        StringBuilder sb = new StringBuilder();
        int cp;
        while ((cp = rd.read()) != -1) {
            sb.append((char) cp);
        }
        return sb.toString();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("regionID", "avgSentiment", "tweetID", "regionJSON", "weatherJSON"));
    }

    private Double jsonToDouble(Object obj) {
        if(obj instanceof Long) {
            return ((Long) obj).doubleValue();
        } else {
            return (Double) obj;
        }
    }
}
