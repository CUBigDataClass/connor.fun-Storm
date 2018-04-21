/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package fun.connor.storm;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Map;

import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.apache.storm.shade.org.json.simple.JSONArray;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class SortBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 177788290277634253L;
    private static final Logger LOG = LoggerFactory.getLogger(SortBolt.class);
    private transient CharsetDecoder decoder;

    private static class Coords {
        Coords(){
            this.latitude = 0.0;
            this.longitude = 0.0;
        }

        public Double latitude;
        public Double longitude;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        decoder = Charset.forName("UTF-8").newDecoder();
    }
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // TODO: PARSE DATA FROM KAFKA - will get full tweet JSON here
        ByteBuffer buffer = ByteBuffer.wrap(input.getBinary(0));
        String data = null; 
        try {
            data = decoder.decode(buffer).toString();
        } catch (CharacterCodingException e) {
            LOG.error("Exception when decoding record ", e);
        }

        // TODO: fetch locations.json, parse them
        // TODO: Compare against time to see if we need a new set
        // Sorry for this next line
        String locationJSON = "[{\"name\": \"Boulder\",\"ID\": \"BLD0\",\"centerLat\": 40.015,\"centerLon\": -105.2705,\"north\": 40.15992753623188,\"east\": -105.45941607298043,\"south\": 39.87007246376812,\"west\": -105.08158392701955},{\"name\": \"New York\",\"ID\": \"NY0\",\"centerLat\": 40.7128,\"centerLon\": -74.006,\"north\": 40.857727536231884,\"east\": -74.19688190088912,\"south\": 40.56787246376812,\"west\": -73.81511809911086},{\"name\": \"Los Angeles\",\"ID\": \"LA0\",\"centerLat\": 34.0522,\"centerLon\": -118.2437,\"north\": 34.19712753623188,\"east\": -118.41833061260624,\"south\": 33.907272463768116,\"west\": -118.06906938739377},{\"name\": \"Seattle\",\"ID\": \"SEA0\",\"centerLat\": 47.6062,\"centerLon\": -122.3321,\"north\": 47.751127536231884,\"east\": -122.54669728390833,\"south\": 47.46127246376812,\"west\": -122.11750271609168}]";

        // Parse tweet JSON
        JSONParser parser = new JSONParser();
        Object obj = null;
        try {
            obj = parser.parse(data);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        JSONObject jsonObject = (JSONObject) obj;

        // Get relevant fields - ID, location, and text
        String tweetID = (String) jsonObject.get("id_str");

        // Get text
        JSONObject tweetFullObj = (JSONObject) jsonObject.get("extended_tweet");
        String tweetFullText = (String) jsonObject.get("text");
        if(tweetFullObj != null ) {
            tweetFullText = (String) tweetFullObj.get("full_text");
        }

        // Get location
        JSONObject coordObj = (JSONObject) jsonObject.get("coordinates");
        // Convert place coords to simple lat/long
        JSONObject placeObj = (JSONObject) jsonObject.get("place");
        JSONObject boundingBox = (JSONObject) placeObj.get("bounding_box");
        JSONArray boxCoords = (JSONArray) boundingBox.get("coordinates");
        
        Coords tweetLoc = this.boxToLatLon(boxCoords);

        if(coordObj != null) {
            JSONArray coordArray = (JSONArray) coordObj.get("coordinates");
            tweetLoc.latitude = ((Double) coordArray.get(0));
            tweetLoc.longitude = ((Double) coordArray.get(1));
        }

        // Alright, now sort it given our list of regions.
        // Parse JSON, get JSON array
        Object regionObj = null;
        try {
            regionObj = parser.parse(locationJSON);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        JSONArray locationObj = (JSONArray) regionObj;
        JSONObject region = coordsToRegion(tweetLoc, locationObj);

        if(region == null) return; // Don't emit, tweet doesn't belong in a region

        LOG.info("SortBolt got tweet: from region " + region.get("ID") + " with id " + tweetID);
        collector.emit(new Values(region.get("ID"), tweetFullText, tweetID, region.toJSONString()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("regionID", "tweetText", "tweetID", "regionJSON"));
    }

    private JSONObject coordsToRegion(Coords loc, JSONArray regions) {
        for(Object regionObj : regions.toArray()) {
            JSONObject regionJSON = (JSONObject) regionObj;
            Double north = (Double) regionJSON.get("north");
            Double south = (Double) regionJSON.get("south");
            Double west = (Double) regionJSON.get("west");
            Double east = (Double) regionJSON.get("east");

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
                avgCoords.latitude += ((Double) coordArray.get(0));
                avgCoords.longitude += ((Double) coordArray.get(1));
            }
        }
        avgCoords.latitude = avgCoords.latitude / 4;
        avgCoords.longitude = avgCoords.longitude / 4;

        return avgCoords;
    }
}
