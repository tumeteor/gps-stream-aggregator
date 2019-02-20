package osrm;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class Utils {

    /**
     * coordinates	String of format  {longitude},{latitude};{longitude},{latitude}[;{longitude},{latitude} ...] or  polyline({polyline})
     * @param raw from stream aggregation, in the form of lon,lat\tlon,lat..
     * @return
     */
    public static String parseCoordinate(String raw) {
        String result = "";
        String[] coor_pairs = raw.split("\t|\\s+");
        for (String coor_pair : coor_pairs) {
            String[] _coor_pair = coor_pair.split(",");
            String lon = _coor_pair[0];
            String lat = _coor_pair[1];

            result +=  lon + "," + lat + ";";
        }
        // dummy remove extra ; at the end
        return StringUtils.substring(result, 0, result.length() - 1);
    }

    /**
     * Parse data to json
     * @param raw: raw coordinates from the raw GPS data / SUBJECTIVE to change
     * @return string json
     */
    public static String parseCoordinateJson(String raw) {
        JSONObject json = new JSONObject();
        JSONArray coordList = new JSONArray();
        String[] coor_pairs = raw.split("\t|\\s+");
        for (String coor_pair : coor_pairs) {
            String[] _coor_pair = coor_pair.split(",");
            Double lon = Double.parseDouble(_coor_pair[0]);
            Double lat = Double.parseDouble(_coor_pair[1]);
            JSONArray coord = new JSONArray();
            coord.put(lon);
            coord.put(lat);
            coordList.put(coord);
        }
        json.put("coordinates", coordList);
        return json.toString();
    }

    /**
     * Dummy parsing to not produce a long trace - for testing OSRM map-matching only
     * coordinates	String of format  {longitude},{latitude};{longitude},{latitude}[;{longitude},{latitude} ...] or  polyline({polyline})
     * @param raw from stream aggregation, in the form of lon,lat\tlon,lat..
     * @return
     */
    public static String dummyParseCoordinate(String raw) {
        String result = "";
        String[] coor_pairs = raw.split("\t|\\s+");
        int limit_length = 5;
        int idx = 0;
        for (String coor_pair : coor_pairs) {
            String[] _coor_pair = coor_pair.split(",");
            String lon = _coor_pair[0];
            String lat = _coor_pair[1];

            result +=  lon + "," + lat + ";";
            idx ++;
            if (idx >= limit_length) break;
        }
        // dummy remove extra ; at the end
        return StringUtils.substring(result, 0, result.length() - 1);
    }


    /**
     * Convert a JSONSE string to pretty print version
     * @param json
     * @return
     */
    public static String toPrettyFormat(org.json.JSONObject json)
    {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String prettyJson = gson.toJson(json);

        return prettyJson;
    }

    /**
     * Merge multiple json objects
     * @param json1
     * @param json2
     * @return
     */
    public static JSONObject mergeJSONObjects(JSONObject json1, JSONObject json2) {
        JSONObject mergedJSON = new JSONObject();
        try {
            mergedJSON = new JSONObject(json1, JSONObject.getNames(json1));
            for (String crunchifyKey : JSONObject.getNames(json2)) {
                mergedJSON.put(crunchifyKey, json2.get(crunchifyKey));
            }

        } catch (JSONException e) {
            throw new RuntimeException("JSONSE Exception" + e);
        }
        return mergedJSON;
    }

    public static void main (String[] args) {
        String raw = "13.43605,52.417229999999996\t13.436279999999998,52.41727\t13.436520000000002,52.4173";
        System.out.println(Utils.parseCoordinate(raw));
    }
}
