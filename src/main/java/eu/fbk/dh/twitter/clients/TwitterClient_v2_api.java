package eu.fbk.dh.twitter.clients;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TweetsApi;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.*;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class TwitterClient_v2_api {
    private HashMap<String, String> options;
    private TwitterApi apiInstance;
    private Set<String> tweetFields = new HashSet<>(Arrays.asList("attachments,author_id,context_annotations,created_at,conversation_id,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,source,text,withheld,reply_settings".split(",")));
    private Set<String> expansions = new HashSet<>(Arrays.asList("referenced_tweets.id,referenced_tweets.id.author_id,entities.mentions.username,attachments.poll_ids,attachments.media_keys,in_reply_to_user_id,geo.place_id,author_id".split(",")));
    // do not add profile_image_url
    // https://github.com/twitterdev/twitter-api-java-sdk/issues/30
    private Set<String> userFields = new HashSet<>(Arrays.asList("created_at,description,entities,location,name,pinned_tweet_id,protected,url,username,verified,withheld,public_metrics".split(",")));

    protected static final Logger logger = LogManager.getLogger();

    //    private static Integer MAX_RESULTS = 100;
//    private static Integer MAX_PAGES_FOR_ONE_SEARCH = 5;
    private static Integer STREAM_LOG_AMOUNT = 50;

    private long tweetsFromStream = 0;

    public TwitterClient_v2_api(HashMap<String, String> options) {
        this.options = options;
        apiInstance = new TwitterApi(new TwitterCredentialsBearer(options.get("twitter.bearer")));
    }

    /*
     * This method calls the filtered stream endpoint and streams Tweets from it
     * */
    public void connectStream() throws IOException, URISyntaxException, InterruptedException {

        logger.info("Starting stream");

        try {
            InputStream result = apiInstance.tweets().searchStream()
                    .tweetFields(tweetFields)
                    .expansions(expansions)
                    .userFields(userFields)
                    .execute();
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(result));
                String line = reader.readLine();
                while (line != null) {
                    if (line.isEmpty()) {
                        logger.trace("==> Empty line");
                        line = reader.readLine();
                        continue;
                    }
                    streamToken();
//                    logger.debug(line);

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
                    } catch (JSONException e) {
                        logger.error("JSON error (is Bearer token correct?): " + e.getMessage());
                        Thread.sleep(Crawler.SLEEP_MS);
                    }

                    line = reader.readLine();
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(e);
            }
        } catch (ApiException e) {
            System.err.println("Exception when calling TweetsApi#searchStream");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }

    public void close() {
//        try {
//            isClosed.set(true);
//            if (reader != null) {
//                logger.info("Closing reader");
//                reader.close();
//            }
//            if (inputStream != null) {
//                logger.info("Closing stream");
//                inputStream.close();
//            }
//            if (connection != null) {
//                logger.info("Closing connection");
//                connection.close();
//            }
//            if (response != null) {
//                logger.info("Closing response");
//                response.close();
//            }
//            if (socket != null) {
//                logger.info("Closing socket");
//                socket.close();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    protected abstract void processTweet(JSONObject json, JSONArray rules) throws InterruptedException;

    protected abstract void saveToken(Map<String, String> ids, String next_token);

    protected abstract void streamToken();

    /*
     * This method calls the full-archive search endpoint with a the search term passed to it as a query parameter
     * */
    public boolean search(ArrayList<NameValuePair> queryParameters, Map<String, String> ids, String next_token) throws IOException, URISyntaxException, InterruptedException {

        // todo: optimize this code
        Map<String, String> pars = new HashMap<>();
        int numPages = 0;
        JSONObject rule = new JSONObject();
        for (NameValuePair queryParameter : queryParameters) {
            rule.put(queryParameter.getName(), queryParameter.getValue());
            pars.put(queryParameter.getName(), queryParameter.getValue());
        }
        JSONArray rules = new JSONArray();
        rules.put(rule);

        TweetsApi.APItweetsFullarchiveSearchRequest query = apiInstance.tweets().tweetsFullarchiveSearch(pars.get("query"));
        OffsetDateTime start_time = OffsetDateTime.parse(pars.get("start_time"));
        // just to avoid that start_time == end_time
        start_time = start_time.minus(Duration.ofSeconds(10));
        query = query.startTime(start_time);
        query = query.endTime(OffsetDateTime.parse(pars.get("end_time")));
        query = query.maxResults(Integer.parseInt(options.get("tweet.max_results")));

        // Do not break this loop, otherwise the tag will be marked as done
        while (next_token != null) {

            if (++numPages > Integer.parseInt(options.get("tweet.max_pages_for_one_search"))) {
                return false;
            }

            if (next_token.length() > 0) {
                saveToken(ids, next_token);
                query = query.nextToken(next_token);
                logger.trace("Token: {} - IDs: {}", next_token, ids);
            }


            query = query
                    .tweetFields(tweetFields)
                    .expansions(expansions)
                    .userFields(userFields);
            try {
                Get2TweetsSearchAllResponse result = query.execute();
                String searchResponse = result.toJson();
                try {
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
                } catch (Exception e) {
                    e.printStackTrace();
                }

                Thread.sleep(Long.parseLong(options.get("tweet.sleep_search_ms")));
            } catch (ApiException e) {
                if (e.getCode() == 429) {
                    logger.error("Too many requests, sleeping...");
                    Thread.sleep(10000);
                } else {
                    System.err.println("Exception when calling TweetsApi#tweetsFullarchiveSearch");
                    System.err.println("Status code: " + e.getCode());
                    System.err.println("Reason: " + e.getResponseBody());
                    System.err.println("Response headers: " + e.getResponseHeaders());
                    e.printStackTrace();
                }
            } catch (JsonIOException e) {
                logger.error("Error, waiting to retry");
                System.out.println(start_time);
                System.out.println(OffsetDateTime.parse(pars.get("end_time")));
                System.out.println(next_token);
                System.out.println(pars);
                e.printStackTrace();
                Thread.sleep(10000);
            }
        }

        return true;
    }

    public void deleteAllRules() throws URISyntaxException, IOException {
        BiMap<String, String> rules = getRules(null);
        List<String> rulesToDelete = new ArrayList<>(rules.keySet());
        deleteRules(rulesToDelete);
    }

    public BiMap<String, String> getRules(String prefix) throws URISyntaxException, IOException {
        BiMap<String, String> rules = HashBiMap.create();

        try {
            RulesLookupResponse result = apiInstance.tweets().getRules().execute();
            if (result.getData() != null) {
                for (Rule rule : result.getData()) {
                    String tag = rule.getTag();
                    if (prefix != null && !tag.startsWith(prefix + "-")) {
                        continue;
                    }
                    rules.put(rule.getId(), rule.getValue());
                }
            } else {
                logger.debug("No rules");
            }
        } catch (ApiException e) {
            System.err.println("Exception when calling TweetsApi#getRules");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }

        return rules;
    }

    public void runRules(AddOrDeleteRulesRequest addOrDeleteRulesRequest) {
        try {
            AddOrDeleteRulesResponse result = apiInstance.tweets().addOrDeleteRules(addOrDeleteRulesRequest).execute();
        } catch (ApiException e) {
            System.err.println("Exception when calling TweetsApi#addOrDeleteRules");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }

    }

    /*
     * Helper method to create rules for filtering
     * */
    public void createRules(Collection<String> rules, String prefix) throws URISyntaxException, IOException {
        if (rules.size() == 0) {
            return;
        }


        AddRulesRequest addRulesRequest = new AddRulesRequest();

        for (String key : rules) {
            String tag = prefix + "-" + key;
            RuleNoId ruleNoId = new RuleNoId();
            ruleNoId.setTag(tag);
            ruleNoId.setValue(key);
            addRulesRequest.addAddItem(ruleNoId);
        }

        AddOrDeleteRulesRequest addOrDeleteRulesRequest = new AddOrDeleteRulesRequest(addRulesRequest);
        runRules(addOrDeleteRulesRequest);
    }

    /*
     * Helper method to delete rules
     * */
    public void deleteRules(List<String> existingRules) throws URISyntaxException, IOException {
        if (existingRules.size() == 0) {
            return;
        }

        DeleteRulesRequestDelete deleteRulesRequestDelete = new DeleteRulesRequestDelete();
        deleteRulesRequestDelete.setIds(existingRules);
        DeleteRulesRequest deleteRulesRequest = new DeleteRulesRequest();
        deleteRulesRequest.delete(deleteRulesRequestDelete);
        AddOrDeleteRulesRequest addOrDeleteRulesRequest = new AddOrDeleteRulesRequest(deleteRulesRequest);

        runRules(addOrDeleteRulesRequest);
    }

}
