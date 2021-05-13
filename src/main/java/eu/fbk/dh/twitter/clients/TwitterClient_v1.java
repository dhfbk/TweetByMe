package eu.fbk.dh.twitter.clients;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterClient_v1 {
    private Twitter twitter;
    private static int DEFAULT_TIMEOUT = 2000;

    public TwitterClient_v1(String consumer_key, String consumer_secret, String access_token, String access_token_secret) {
        this(consumer_key, consumer_secret, access_token, access_token_secret, DEFAULT_TIMEOUT);
    }

    public TwitterClient_v1(String consumer_key, String consumer_secret, String access_token, String access_token_secret, int time_out) {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setHttpReadTimeout(time_out)
                .setHttpConnectionTimeout(time_out)
                .setHttpStreamingReadTimeout(time_out)
                .setOAuthConsumerKey(consumer_key)
                .setOAuthConsumerSecret(consumer_secret)
                .setOAuthAccessToken(access_token)
                .setOAuthAccessTokenSecret(access_token_secret);
        TwitterFactory tf = new TwitterFactory(cb.build());
        twitter = tf.getInstance();
    }


    public Twitter getTwitter() {
        return twitter;
    }
}
