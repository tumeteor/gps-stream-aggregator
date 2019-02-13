package osrm;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;

public class Match {

    //String OSRM_SERVER = "http://0.0.0.0:5000";
    String OSRM_SERVER = "https://routing.develop.otonomousmobility.com/car/";

    HttpClient httpClient;



    public Match() {
        httpClient = HttpClients.createDefault();
    }

    /**
     * Constructor for online mode
     * @param osrm_server: remote OSRM server instance
     */
    public Match(String osrm_server) {
        this.OSRM_SERVER = osrm_server;
    }

    /**
     * Snaps the supplied coordinates to the road network, and returns the most likely route given the points.
     *
     * @param coordinates The string containing comma separated lon/lat. Multiple coordinate pairs are separated by a semicolon.
     * @return A JSON object containing the response code, an array of waypoint objects, and an array of route objects.
     */
    public JSONObject matchPoints(String coordinates) {
        String url = String.format(OSRM_SERVER + "/match/v1/car/%s?geometries=geojson&overview=full", coordinates);
        System.out.println(url);

        return fetchResult(url);
    }

    /**
     * Snaps the supplied coordinates to the road network, and returns the most likely route given the points.
     *
     * @param coordinates The string containing comma separated lon/lat. Multiple coordinate pairs are separated by a semicolon.
     * @param profile     The mode of travel. Valid values are 'car', 'bike' and 'foot'.
     * @return A JSON object containing the response code, an array of waypoint objects, and an array of route objects.
     */
    public JSONObject matchPoints(String coordinates, String profile) {
        String url = String.format(OSRM_SERVER + "/match/v1/%s/%s?geometries=geojson&overview=full", profile, coordinates);
        return fetchResult(url);
    }


    public JSONObject matchPointWithJson(String coordinates) {
        HttpPost post = new HttpPost(OSRM_SERVER + "/match");
        post.setHeader("Content-Type", "application/json");
        post.setHeader("Accept", "application/json");
        post.setHeader("X-Stream" , "true");

        String jsonData = Utils.parseCoordinateJson(coordinates);

        System.out.println(jsonData);
        try {
            return executeHttpRequest(jsonData, post);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     * @param jsonData
     * @param httpPost
     * @throws UnsupportedEncodingException
     * @throws IOException
     */
    JSONObject executeHttpRequest(String jsonData, HttpPost httpPost)  throws UnsupportedEncodingException, IOException
    {
        HttpResponse response=null;
        String line = "";
        StringBuffer result = new StringBuffer();
        httpPost.setEntity(new StringEntity(jsonData));
        httpPost.addHeader("accept", "application/json");
//        httpPost.addHeader(BasicScheme.authenticate(
//                new UsernamePasswordCredentials("NGUYTU3", "Twig285ache743#"),
//                "UTF-8", false));
        HttpClient client = HttpClientBuilder.create().build();


        response = client.execute(httpPost);
        System.out.println(response);
        return new JSONObject(IOUtils.toString(response.getEntity().getContent(), "UTF-8"));
    }


    public JSONObject fetchResult(String url) {
        JSONObject result = null;
        try {
            URIBuilder builder = new URIBuilder(url);
            URI uri = builder.build();
            HttpGet request = new HttpGet(uri);
            request.addHeader("accept", "application/json");
//            request.addHeader(BasicScheme.authenticate(
//                    new UsernamePasswordCredentials("NGUYTU3", "Twig285ache743#"),
//                    "UTF-8", false));
            HttpResponse response = httpClient.execute(request);
            result = new JSONObject(IOUtils.toString(response.getEntity().getContent(), "UTF-8"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return result;
    }

    public static void main (String[] args) {
        Match osrm_match = new Match();
        String coordinates = "13.43605,52.417229999999996\t13.436279999999998,52.41727";

        JSONObject map_matched = osrm_match.matchPointWithJson(coordinates);

        if (map_matched != null)
            System.out.println(Utils.toPrettyFormat(map_matched));

    }
}
