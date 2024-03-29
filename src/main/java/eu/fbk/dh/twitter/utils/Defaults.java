package eu.fbk.dh.twitter.utils;

import java.util.HashMap;

public class Defaults {
    public static HashMap<String, String> getDefaultOptions() {
        return defaultOptions;
    }

    static HashMap<String, String> defaultOptions = new HashMap<>();
    static {
//        defaultOptions.put("elastic.hostname", "localhost");
//        defaultOptions.put("elastic.port", "9200");
//        defaultOptions.put("elastic.scheme", "http");

        defaultOptions.put("tweet.collect_previous_days", "7");
        defaultOptions.put("tweet.collect_next_days", "7");
        defaultOptions.put("tweet.group_by", "10");

        // This is an estimation based on the Twitter limits
        // https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-all
        defaultOptions.put("tweet.sleep_search_ms", "2500");

        defaultOptions.put("tweet.max_results", "100");
        defaultOptions.put("tweet.max_pages_for_one_search", "5");
        defaultOptions.put("tweet.max_attempts_external_app", "5");

        defaultOptions.put("tweet.converter_host", "dh-hetzner.fbk.eu");
        defaultOptions.put("tweet.converter_port", "9206");
        defaultOptions.put("tweet.converter_protocol", "http");
        defaultOptions.put("tweet.converter_address", "");

        defaultOptions.put("tweet.ppp_host", "dh-hetzner.fbk.eu");
        defaultOptions.put("tweet.ppp_port", "9205");
        defaultOptions.put("tweet.ppp_protocol", "http");
        defaultOptions.put("tweet.ppp_address", "");

        defaultOptions.put("tweet.alive_interval_minutes", "1");
        defaultOptions.put("tweet.options_update_interval_minutes", "10");
        defaultOptions.put("tweet.options_close_stream_minutes", "0");
        defaultOptions.put("tweet.trends_update_interval_minutes", "5");
        defaultOptions.put("tweet.trends_location", "23424853");
        defaultOptions.put("tweet.trends_number", "5");
        defaultOptions.put("tweet.trends_lang", "it");
        defaultOptions.put("tweet.trends_number_update", "5");
        defaultOptions.put("tweet.trends_hashtag_only", "true");

        defaultOptions.put("twitter.consumer_key", "");
        defaultOptions.put("twitter.consumer_secret", "");
        defaultOptions.put("twitter.access_token", "");
        defaultOptions.put("twitter.access_token_secret", "");
        defaultOptions.put("twitter.bearer", "");

        defaultOptions.put("app.empty_db", "0");
        defaultOptions.put("twitter.empty_rules", "0");

        defaultOptions.put("app.run_trend_server", "0");
        defaultOptions.put("app.run_update_previous", "1");
        defaultOptions.put("app.run_crawler", "1");
        defaultOptions.put("app.use_ppp", "1");
    }
}
