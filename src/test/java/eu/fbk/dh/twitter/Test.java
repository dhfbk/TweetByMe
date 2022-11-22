package eu.fbk.dh.twitter;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TweetsApi;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.Get2TweetsSearchAllResponse;

import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Test {
    public static void main(String[] args) {
        Set<String> tweetFields = new HashSet<>(Arrays.asList("attachments,author_id,context_annotations,created_at,conversation_id,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,source,text,withheld,reply_settings".split(",")));
        Set<String> expansions = new HashSet<>(Arrays.asList("referenced_tweets.id,referenced_tweets.id.author_id,entities.mentions.username,attachments.poll_ids,attachments.media_keys,in_reply_to_user_id,geo.place_id,author_id".split(",")));
        Set<String> userFields = new HashSet<>(Arrays.asList("created_at,description,entities,location,name,pinned_tweet_id,profile_image_url,protected,url,username,verified,withheld,public_metrics".split(",")));
        String bearer = "";

        TwitterApi apiInstance = new TwitterApi(new TwitterCredentialsBearer(bearer));
        TweetsApi.APItweetsFullarchiveSearchRequest query = apiInstance.tweets().tweetsFullarchiveSearch("#gfvip lang:it");
        query = query.startTime(OffsetDateTime.parse("2022-09-14T13:33:47Z"));
        query = query.endTime(OffsetDateTime.parse("2022-09-21T13:33:47Z"));
        query = query.maxResults(100);
        query = query
                .tweetFields(tweetFields)
                .expansions(expansions)
                .userFields(userFields);
        query = query.nextToken("b26v89c19zqg8o3fpzbki9yln9ee82c41kwxj91jh49rx");
        try {
            Get2TweetsSearchAllResponse result = query.execute();
        } catch (ApiException e) {
            e.printStackTrace();
        }

    }
}
