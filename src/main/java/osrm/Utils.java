package osrm;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;


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
     * Convert a JSON string to pretty print version
     * @param json
     * @return
     */
    public static String toPrettyFormat(org.json.JSONObject json)
    {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String prettyJson = gson.toJson(json);

        return prettyJson;
    }
}
