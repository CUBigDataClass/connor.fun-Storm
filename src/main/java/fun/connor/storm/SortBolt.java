package fun.connor.storm;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.sql.Timestamp;
import java.util.Map;

import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.shade.org.json.simple.JSONArray;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import sun.reflect.annotation.ExceptionProxy;

public class SortBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 177788290277634253L;
    private static final Logger LOG = LoggerFactory.getLogger(SortBolt.class);
    private transient CharsetDecoder decoder;
    private String locationEndpoint;
    private Timestamp timestamp;
    private JSONArray regions;

    private static class Coords {
        Coords(){
            this.latitude = 0.0;
            this.longitude = 0.0;
        }

        public Double latitude;
        public Double longitude;
    }

    public SortBolt(String locationEndpoint) {
        this.timestamp = new Timestamp(System.currentTimeMillis() - 1);
        this.locationEndpoint = locationEndpoint;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        decoder = Charset.forName("UTF-8").newDecoder();
    }
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        ByteBuffer buffer = ByteBuffer.wrap(input.getBinary(0));
        String data = null; 
        try {
            data = decoder.decode(buffer).toString();
        } catch (CharacterCodingException e) {
            LOG.error("Exception when decoding record ", e);
        }

        JSONParser parser = new JSONParser();

        // Parse tweet JSON
        Object obj = null;
        try {
            obj = parser.parse(data);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        JSONObject jsonObject = (JSONObject) obj;

        // Get relevant fields - ID, location, and text
        String tweetID = (String) jsonObject.get("id_str");
        Boolean sensitivity = (Boolean) jsonObject.get("possibly_sensitive");

        // Get text
        JSONObject tweetFullObj = (JSONObject) jsonObject.get("extended_tweet");
        String tweetFullText = (String) jsonObject.get("text");
        if(tweetFullObj != null ) {
            tweetFullText = (String) tweetFullObj.get("full_text");
        }

        Coords tweetLoc = new Coords();

        // Get location
        JSONObject coordObj = (JSONObject) jsonObject.get("coordinates");
        // Convert place coords to simple lat/long
        JSONObject placeObj = (JSONObject) jsonObject.get("place");
        if(placeObj != null) {
            JSONObject boundingBox = (JSONObject) placeObj.get("bounding_box");
            if(boundingBox != null) {
                JSONArray boxCoords = (JSONArray) boundingBox.get("coordinates");
                if(boxCoords != null) {
                    tweetLoc = this.boxToLatLon(boxCoords);
                }
            }
        }

        // Alright, now sort it given our list of regions.
        if(coordObj != null) {
            JSONArray coordArray = (JSONArray) coordObj.get("coordinates");
            tweetLoc.latitude = this.jsonToDouble(coordArray.get(0));
            tweetLoc.longitude =  this.jsonToDouble(coordArray.get(1));
        }

        try {
            JSONArray locationObj = this.getRegions();
            JSONObject region = coordsToRegion(tweetLoc, locationObj);

            if(region == null) return; // Don't emit, tweet doesn't belong in a region

            LOG.info("SortBolt got tweet: from region " + region.get("ID") + " with id " + tweetID);
            collector.emit(new Values(region.get("ID"), tweetFullText, tweetID, region, sensitivity));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("regionID", "tweetText", "tweetID", "regionJSON", "possiblySensitive"));
    }

    private JSONArray getRegions() throws IOException  {
        // See if two minutes have passed
        if(this.timestamp.before(new Timestamp(System.currentTimeMillis()))) {
            // Sorry for this next line
            InputStream is = new URL(this.locationEndpoint + "/locations").openStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
            String locationJSON = readAll(rd);

            // Parse JSON, get JSON array
            Object regionWrapper = null;
            Object regionObj = null;
            JSONParser parser = new JSONParser();
            try {
                regionWrapper = parser.parse(locationJSON);
                regionObj = parser.parse((String) regionWrapper);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            LOG.info("SortBolt got Region: " + regionObj.toString());

            this.regions = (JSONArray) regionObj;
            this.timestamp = new Timestamp(System.currentTimeMillis() + 120000);
        }

        return this.regions;
    }

    private static String readAll(Reader rd) throws IOException {
        StringBuilder sb = new StringBuilder();
        int cp;
        while ((cp = rd.read()) != -1) {
            sb.append((char) cp);
        }
        return sb.toString();
    }

    private Double jsonToDouble(Object obj) {
        if(obj instanceof Long) {
            return ((Long) obj).doubleValue();
        } else {
            return (Double) obj;
        }
    }

    private JSONObject coordsToRegion(Coords loc, JSONArray regions) {
        for(Object regionObj : regions.toArray()) {
            JSONObject regionJSON = (JSONObject) regionObj;
            Double north = this.jsonToDouble(regionJSON.get("north"));
            Double south = this.jsonToDouble(regionJSON.get("south"));
            Double west = this.jsonToDouble(regionJSON.get("west"));
            Double east = this.jsonToDouble(regionJSON.get("east"));

            if(north > loc.longitude && loc.longitude > south && west > loc.latitude && loc.latitude > east) {
                return regionJSON;
            }
        }
        // No matches, return null
        return null;
    } 

    private Coords boxToLatLon(JSONArray boxCoords) {
        JSONArray coordWrapper = (JSONArray) boxCoords.get(0);
        // Now we should have four points - add up and average
        Coords avgCoords = new Coords();

        if(coordWrapper == null) return avgCoords;

        for(Object coordObj : coordWrapper.toArray()) {
            JSONArray coordArray = (JSONArray) coordObj;

            if(coordObj != null) {
                avgCoords.latitude += this.jsonToDouble(coordArray.get(0));
                avgCoords.longitude += this.jsonToDouble(coordArray.get(1));
            }
        }
        avgCoords.latitude = avgCoords.latitude / 4;
        avgCoords.longitude = avgCoords.longitude / 4;

        return avgCoords;
    }
}
