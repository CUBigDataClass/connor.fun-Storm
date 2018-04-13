package fun.connor.storm;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;

public class WeatherBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WeatherBolt.class);
    private KinesisProducer kinesisProducer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        KinesisProducer kinesis = new KinesisProducer();
        this.kinesisProducer = kinesis;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // Open up weather to pre-prescribed regionID
        String regionJSON = (String) input.getValue(3);

        JSONParser parser = new JSONParser();
        Object obj = null;
        try {
            obj = parser.parse(regionJSON);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        JSONObject jsonObject = (JSONObject) obj;
        Double lat = (Double) jsonObject.get("centerLat");
        Double lon = (Double) jsonObject.get("centerLon");

        // Call the weather API
        String key = System.getenv("WEATHER_KEY");
        String url = String.format("https://api.openweathermap.org/data/2.5/weather?lon=%f&lat=%f&units=imperial&APPID=%s",
                lon, lat, key);

        String regionID = (String) input.getValue(0);
        Float avgSentiment = (Float) input.getValue(1);
        String tweetID = (String) input.getValue(2);

        try {
            InputStream is = new URL(url).openStream();
            try {
                BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
                String weatherJSON = readAll(rd);

                LOG.info("WeatherBolt got region: regionID=" + regionID + " with average sentiment of " + avgSentiment + " and weather of " + weatherJSON);

                Values output = new Values(regionID, avgSentiment, tweetID, regionJSON, weatherJSON);

                // Put some records
                ByteBuffer data = ByteBuffer.wrap(output.toString().getBytes("UTF-8"));
                // doesn't block! tnx kpl, hope this works
                this.kinesisProducer.addUserRecord("connorfun-frontend", "0", data);

                // Output fields
                collector.emit(output);
            } finally {
                is.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

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
        declarer.declare(new Fields("regionID", "avgSentiment", "tweetID", "regionJSON", "weatherJSON"));
    }
}
