package eu.fbk.dh.twitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.fbk.dh.twitter.clients.GenericClient;
import eu.fbk.dh.twitter.clients.TwitterClient_v2_api;
import eu.fbk.dh.twitter.mongo.Tweet;
import eu.fbk.dh.twitter.mongo.TweetRepository;
import eu.fbk.dh.twitter.tables.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class TwitterClientSave_v2 extends TwitterClient_v2_api {

    private HashMap<String, String> options;

//    private static Integer MAX_ATTEMPTS = 5;
    private static Integer ATTEMPTS_WAIT_MS = 100;
    protected GenericClient twitterConverter, ppp;
    protected long session_id;
    private long lastStreamToken = 0;
    private long interval;
    private TweetRepository tweetRepository;

    private SessionRepository sessionRepository;
    private TagRepository tagRepository;
    private SessionTagRepository sessionTagRepository;
    private ForeverTagRepository foreverTagRepository;

    String pattern = "EEE MMM dd HH:mm:ss Z yyyy";
    SimpleDateFormat format = new SimpleDateFormat(pattern);

    public void setSession_id(long session_id) {
        this.session_id = session_id;
    }

    public TwitterClientSave_v2(HashMap<String, String> options, TweetRepository tweetRepository,
                                GenericClient twitterConverter, GenericClient ppp,
                                SessionRepository sessionRepository, TagRepository tagRepository,
                                SessionTagRepository sessionTagRepository, ForeverTagRepository foreverTagRepository) {
        super(options);
        this.options = options;
        this.tweetRepository = tweetRepository;
        this.twitterConverter = twitterConverter;
        this.ppp = ppp;
        this.interval = Long.parseLong(options.get("tweet.alive_interval_minutes")) * 60;
        this.sessionRepository = sessionRepository;
        this.tagRepository = tagRepository;
        this.sessionTagRepository = sessionTagRepository;
        this.foreverTagRepository = foreverTagRepository;
    }

    @Override
    protected void streamToken() {
        try {
            long now = System.currentTimeMillis() / 1000L;
            if (now - interval > lastStreamToken) {
                lastStreamToken = now;
                logger.info("Updating alive data");
                Optional<Session> optionalSession = sessionRepository.findById(session_id);
                if (optionalSession.isPresent()) {
                    Session session = optionalSession.get();
                    session.setEnd_time(now);
                    sessionRepository.save(session);
                } else {
                    throw new Exception("Missing session_id");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    protected void saveToken(Map<String, String> ids, String next_token) {

        String tag = ids.get("tag");
        String lang = ids.get("lang");

        try {
            if (ids.containsKey("forever_time")) {
                long insert_time = Long.parseLong(ids.get("forever_time"));
                foreverTagRepository.updateNextToken(next_token, tag, lang, insert_time);
                logger.debug("Updated next_token data ({}) for forever-tag {} and insert_time {}", next_token, tag, insert_time);
            }

            if (ids.containsKey("insert_time")) {
                long insert_time = Long.parseLong(ids.get("insert_time"));
                tagRepository.updateNextToken(next_token, tag, lang, insert_time);
                logger.debug("Updated next_token data ({}) for tag {} and insert_time {}", next_token, tag, insert_time);
            }

            if (ids.containsKey("session_id")) {
                long session_id = Long.parseLong(ids.get("session_id"));
                if (tag == null) {
                    String[] tags = ids.get("tags").split("\\s+");
                    for (String thisTag : tags) {
                        sessionTagRepository.updateNextToken(next_token, thisTag, lang, session_id);
                        logger.debug("Updated next_token data ({}) for tag {} and session_id {}", next_token, thisTag, session_id);
                    }
                } else {
                    sessionTagRepository.updateNextToken(next_token, tag, lang, session_id);
                    logger.debug("Updated next_token data ({}) for tag {} and session_id {}", next_token, tag, session_id);
                }
            }
        } catch (Exception e) {
            logger.error("{}", e.getMessage());
            e.printStackTrace();
        }

    }

    @Override
    protected void processTweet(JSONObject json, JSONArray rules) throws InterruptedException {
        String result = successfullRequest(twitterConverter, json.toString());
        if (result == null) {
            return;
        }

        JSONArray json_v1 = new JSONArray(result);
        if (json_v1.length() == 0) {
            return;
        }

        logger.debug("Processing {} tweet(s): {}", json_v1.length(), json_v1.getJSONObject(0).getString("id_str"));

        for (int i = 0; i < json_v1.length(); i++) {
            JSONObject thisTweet = json_v1.getJSONObject(i);

            thisTweet.put("matching_rules", rules);
            thisTweet.put("session_id", session_id);
            String text = thisTweet.getString("text");

            // Emotions
            if (ppp != null) {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("lang", thisTweet.getString("lang"));
                jsonObject.put("text", text);
                String emotions = successfullRequest(ppp, jsonObject.toString());
                if (emotions != null) {
                    JSONObject emotionsObject = new JSONObject(emotions);
                    thisTweet.put("emotions", emotionsObject);
                }
            }

            String created_at = thisTweet.getString("created_at");
            Date javaDate = null;
            try {
                javaDate = format.parse(created_at);
                long ts = javaDate.getTime() / 1000;
                thisTweet.put("created_at_ts", ts);
            } catch (Exception e) {
                logger.error(e.getMessage());
                long ts = System.currentTimeMillis() / 1000;
                thisTweet.put("created_at_ts", ts);
            }

            String id_str = thisTweet.getString("id_str");
            Long id = Long.parseLong(id_str);
            thisTweet.put("id", id);
            String tweet_json = thisTweet.toString();

            if (logger.isTraceEnabled()) {
                logger.trace(id_str + " - " + text.replaceAll("\\n+", " "));
            }
            try {
//                elasticClient.addElement("tweets", id_str, tweet_json);
                ObjectMapper objectMapper = new ObjectMapper();
                Tweet tweet = objectMapper.readValue(tweet_json, Tweet.class);
                tweetRepository.save(tweet);
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }

    }

    private String successfullRequest(GenericClient service, String data) throws InterruptedException {
        String result = null;
        int attempts = 0;
        boolean success = false;
        while (!success && attempts < Integer.parseInt(options.get("tweet.max_attempts_external_app"))) {
            attempts++;
            try {
                result = service.request(data);
                if (attempts > 1) {
                    logger.info("Attempt {} succeded", attempts);
                }
                success = true;
            } catch (Exception e) {
                logger.error(e.getMessage());
                Thread.sleep(ATTEMPTS_WAIT_MS);
            }
        }

        if (!success) {
            return null;
        }

        return result;
    }
}
