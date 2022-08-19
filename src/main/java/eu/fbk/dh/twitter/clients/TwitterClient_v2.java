package eu.fbk.dh.twitter.clients;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import eu.fbk.dh.twitter.runners.Crawler;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class TwitterClient_v2 {
    private String bearerToken;
    private CloseableHttpClient httpClientForStream, httpClientWithTimeout;
    private Socket socket = null;

    public AtomicBoolean isClosed = new AtomicBoolean(false);

    protected static final Logger logger = LogManager.getLogger();

    private static String URI_STREAM_RULES = "https://api.twitter.com/2/tweets/search/stream/rules";
    private static String URI_STREAM = "https://api.twitter.com/2/tweets/search/stream";
    private static String URI_SEARCH = "https://api.twitter.com/2/tweets/search/all";

    private static String MAX_RESULTS = "100";
    private static Integer SLEEP_MS = 500;
    private static Integer MAX_PAGES_FOR_ONE_SEARCH = 5;
    private static Integer STREAM_LOG_AMOUNT = 50;

    private long tweetsFromStream = 0;

    public TwitterClient_v2(String bearerToken) {
        this.bearerToken = bearerToken;
        RequestConfig requestConfigForStream = RequestConfig.custom()
                .setSocketTimeout(60000)
                .setCookieSpec(CookieSpecs.STANDARD).build();
        httpClientForStream = HttpClients.custom().setDefaultRequestConfig(requestConfigForStream).build();

        RequestConfig requestConfigForSearch = RequestConfig.custom()
                .setConnectTimeout(2000)
                .setSocketTimeout(2000)
                .setConnectionRequestTimeout(2000)
                .setCookieSpec(CookieSpecs.STANDARD).build();
        httpClientWithTimeout = HttpClients.custom().setDefaultRequestConfig(requestConfigForSearch).build();
    }

    /*
     * This method calls the filtered stream endpoint and streams Tweets from it
     * */
    public void connectStream() throws IOException, URISyntaxException, InterruptedException {

        logger.info("Starting stream");

        URIBuilder uriBuilder = new URIBuilder(URI_STREAM)
                .addParameter("tweet.fields", "attachments,author_id,context_annotations,created_at,conversation_id,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,source,text,withheld,reply_settings").
                addParameter("expansions", "referenced_tweets.id,referenced_tweets.id.author_id,entities.mentions.username,attachments.poll_ids,attachments.media_keys,in_reply_to_user_id,geo.place_id,author_id").
                addParameter("user.fields", "created_at,description,entities,location,name,pinned_tweet_id,profile_image_url,protected,url,username,verified,withheld,public_metrics");

        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));

        HttpClientContext context = HttpClientContext.create();
        CloseableHttpResponse response = httpClientForStream.execute(httpGet, context);
        ManagedHttpClientConnection connection = context.getConnection(ManagedHttpClientConnection.class);
        socket = connection.getSocket();

        HttpEntity entity = response.getEntity();
        if (null != entity) {
            InputStream inputStream = entity.getContent();
            InputStreamReader stream = new InputStreamReader(inputStream);
            BufferedReader reader = new BufferedReader(stream);
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().length() == 0) {
                    continue;
                }
                streamToken();
                try {
                    JSONObject json = new JSONObject(line);
                    if (json.has("matching_rules")) {
                        JSONArray rules = (JSONArray) json.get("matching_rules");
                        if (++tweetsFromStream >= STREAM_LOG_AMOUNT) {
                            logger.info("Bunch of {} tweets from streaming", tweetsFromStream);
                            tweetsFromStream = 0;
                        }
                        processTweet(json, rules);
                    }
                }
                catch (JSONException e) {
                    logger.error("JSON error (is Bearer token correct?): " + e.getMessage());
                    Thread.sleep(Crawler.SLEEP_MS);
                }
            }
        }
    }

    public void close() {
        try {
            isClosed.set(true);
            if (socket != null) {
                socket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected abstract void processTweet(JSONObject json, JSONArray rules) throws InterruptedException;

    protected abstract void saveToken(Map<String, String> ids, String next_token);

    protected abstract void streamToken();

    /*
     * This method calls the full-archive search endpoint with a the search term passed to it as a query parameter
     * */
    public boolean search(ArrayList<NameValuePair> queryParameters, Map<String, String> ids, String next_token) throws IOException, URISyntaxException, InterruptedException {

        int numPages = 0;
        JSONObject rule = new JSONObject();
        for (NameValuePair queryParameter : queryParameters) {
            rule.put(queryParameter.getName(), queryParameter.getValue());
        }
        JSONArray rules = new JSONArray();
        rules.put(rule);

        queryParameters.add(new BasicNameValuePair("max_results", MAX_RESULTS));

        // Do not break this loop, otherwise the tag will be marked as done
        while (next_token != null) {
            if (isClosed.get()) {
                return false;
            }

            if (++numPages > MAX_PAGES_FOR_ONE_SEARCH) {
                return false;
            }

            ArrayList<NameValuePair> newQueryParameters = new ArrayList<>(queryParameters);
            if (next_token.length() > 0) {
                saveToken(ids, next_token);
                newQueryParameters.add(new BasicNameValuePair("next_token", next_token));
                logger.trace("Token: {} - IDs: {}", next_token, ids);
            }

            URIBuilder uriBuilderSearch = new URIBuilder(URI_SEARCH)
                    .addParameter("tweet.fields", "attachments,author_id,context_annotations,created_at,conversation_id,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,source,text,withheld,reply_settings").
                    addParameter("expansions", "referenced_tweets.id,referenced_tweets.id.author_id,entities.mentions.username,attachments.poll_ids,attachments.media_keys,in_reply_to_user_id,geo.place_id,author_id").
                    addParameter("user.fields", "created_at,description,entities,location,name,pinned_tweet_id,profile_image_url,protected,url,username,verified,withheld,public_metrics");
            uriBuilderSearch.addParameters(newQueryParameters);

            HttpGet httpGet = new HttpGet(uriBuilderSearch.build());
            httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
            httpGet.setHeader("Content-Type", "application/json");

            CloseableHttpResponse response = httpClientWithTimeout.execute(httpGet);
            HttpEntity entity = response.getEntity();
            if (null != entity) {
                String searchResponse = EntityUtils.toString(entity, "UTF-8");
                JSONObject json = new JSONObject(searchResponse);
                if (json.has("errors")) {
                    JSONArray errors = json.getJSONArray("errors");
                    for (int i = 0; i < errors.length(); i++) {
                        JSONObject errorsJSONObject = errors.getJSONObject(i);
                        if (errorsJSONObject.has("message")) {
                            logger.error(errorsJSONObject.getString("message"));
                        }
                    }
                }
                if (json.has("meta")) {
                    JSONObject meta = json.getJSONObject("meta");
                    if (meta.has("next_token")) {
                        next_token = meta.getString("next_token");
                    } else {
                        next_token = null;
                    }
                } else {
                    next_token = null;
                }
                processTweet(json, rules);
            }

            response.close();

            try {
                Thread.sleep(SLEEP_MS);
            } catch (Exception e) {
                if (!isClosed.get()) {
                    logger.error(e.getMessage());
                    logger.error(Thread.currentThread().isInterrupted());
                    e.printStackTrace();
                }
            }
        }

        return true;
    }

    /*
     * Helper method to get existing rules
     * */
//    public BiMap<String, String> getRules() throws URISyntaxException, IOException {
//        return getRules(null);
//    }

    public BiMap<String, String> getRules(String prefix) throws URISyntaxException, IOException {
        BiMap<String, String> rules = HashBiMap.create();
        URIBuilder uriBuilderStream = new URIBuilder(URI_STREAM_RULES);

        HttpGet httpGet = new HttpGet(uriBuilderStream.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpGet.setHeader("content-type", "application/json");
        CloseableHttpResponse response = httpClientWithTimeout.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            JSONObject json = new JSONObject(EntityUtils.toString(entity, "UTF-8"));
            if (json.length() > 1) {
                if (json.has("data")) {
                    JSONArray array = (JSONArray) json.get("data");
                    for (int i = 0; i < array.length(); i++) {
                        JSONObject jsonObject = (JSONObject) array.get(i);
                        String tag = jsonObject.getString("tag");
                        if (prefix != null && !tag.startsWith(prefix + "-")) {
                            continue;
                        }
                        rules.put(jsonObject.getString("id"), jsonObject.getString("value"));
                    }
                } else {
                    logger.error("Unable to read rules (is Bearer token correct?)");
                    System.exit(1);
                }
            }
        }
        response.close();
        return rules;
    }

    public void postJson(JsonObject o) throws URISyntaxException, IOException {
        URIBuilder uriBuilderStream = new URIBuilder(URI_STREAM_RULES);
        HttpPost httpPost = new HttpPost(uriBuilderStream.build());
        httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpPost.setHeader("content-type", "application/json");
        StringEntity body = new StringEntity(o.toString(), StandardCharsets.UTF_8);
        httpPost.setEntity(body);
        CloseableHttpResponse response = httpClientWithTimeout.execute(httpPost);
        response.close();
    }

    /*
     * Helper method to create rules for filtering
     * */
    public void createRules(Collection<String> rules, String prefix) throws URISyntaxException, IOException {
        if (rules.size() == 0) {
            return;
        }

        JsonArray addArray = new JsonArray();
        for (String key : rules) {
            String tag = prefix + "-" + key;
            JsonObject tagObject = new JsonObject();
            tagObject.addProperty("value", key);
            tagObject.addProperty("tag", tag);
            addArray.add(tagObject);
        }
        JsonObject finalObject = new JsonObject();
        finalObject.add("add", addArray);

        postJson(finalObject);
    }

    /*
     * Helper method to delete rules
     * */
    public void deleteRules(List<String> existingRules) throws URISyntaxException, IOException {
        if (existingRules.size() == 0) {
            return;
        }

        JsonArray jsonArray = new JsonArray();
        for (String existingRule : existingRules) {
            jsonArray.add(existingRule);
        }
        JsonObject idsObject = new JsonObject();
        idsObject.add("ids", jsonArray);
        JsonObject finalObject = new JsonObject();
        finalObject.add("delete", idsObject);

        postJson(finalObject);
    }

}
