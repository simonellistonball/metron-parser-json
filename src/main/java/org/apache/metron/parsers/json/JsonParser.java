package org.apache.metron.parsers.json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.metron.parsers.BasicParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonParser extends BasicParser {
	protected static final Logger LOG = LoggerFactory.getLogger(JsonParser.class);
	private static final String MAPPINGS_CONFIG = "fieldMappings";
	private Map<String, String> mappings;
	
	@Override
	public void init() {
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<JSONObject> parse(byte[] rawMessage) {
		List<JSONObject> messages = new ArrayList<>();
		try {
			/*
			 * We need to create a new JSONParser each time because its not
			 * serializable and the parser is created on the storm nimbus node,
			 * then transfered to the workers.
			 */
			JSONParser jsonParser = new JSONParser();
			String rawString = new String(rawMessage, "UTF-8");
			JSONObject rawJson = (JSONObject) jsonParser.parse(rawString);

			// rename other keys based on configuration map
			for (Entry<String, String> e : mappings.entrySet()) {
				rawJson = mutate(rawJson, e.getKey(), e.getValue());
			}
	
			// convert seconds timestamp to milli since epoch
			String strTimestamp = rawJson.get("timestamp").toString();
			if (strTimestamp.length() <= 13) {
				long timestamp = Long.valueOf(strTimestamp) * 1000;
				rawJson.put("timestamp", timestamp);
			}
			
			messages.add(rawJson);
			return messages;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private JSONObject mutate(JSONObject json, String oldKey, String newKey) {
		if (json.containsKey(oldKey)) {
			json.put(newKey, json.remove(oldKey));
		}
		return json;
	}

	@Override
	public void configure(Map<String, Object> parserConfig) {
		mappings = (Map<String,String>) parserConfig.get(MAPPINGS_CONFIG);
	}

}