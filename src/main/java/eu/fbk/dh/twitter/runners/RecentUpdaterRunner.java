package eu.fbk.dh.twitter.runners;

import eu.fbk.dh.twitter.clients.TwitterClient_v2;
import eu.fbk.dh.twitter.tables.SessionTag;
import eu.fbk.dh.twitter.tables.SessionTagRepository;
import eu.fbk.dh.twitter.tables.Tag;
import eu.fbk.dh.twitter.tables.TagRepository;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static eu.fbk.dh.twitter.utils.Utils.unixToTime;

public class RecentUpdaterRunner {
    protected static final Logger logger = LogManager.getLogger();

    TwitterClient_v2 twitterClient_v2;

    SessionTagRepository sessionTagRepository;
    TagRepository tagRepository;

    private AtomicBoolean hasJobs = new AtomicBoolean(true);

    public boolean hasJobs() {
        return hasJobs.get();
    }

    public RecentUpdaterRunner(TwitterClient_v2 twitterClient_v2,
                               SessionTagRepository sessionTagRepository, TagRepository tagRepository) {
        this.twitterClient_v2 = twitterClient_v2;
        this.sessionTagRepository = sessionTagRepository;
        this.tagRepository = tagRepository;
    }

    public void update() throws IOException, URISyntaxException, InterruptedException {
        boolean hasBlockJobs = false;
        boolean hasHistoricalJobs = false;

        List<SessionTag> unfinishedSessionTags = sessionTagRepository.getUnfinishedSessionTags();
        for (SessionTag unfinishedSessionTag : unfinishedSessionTags) {
            hasBlockJobs = true;
            String tag = unfinishedSessionTag.getTag();
            Long session_id = unfinishedSessionTag.getSession_id();
            String next_token = unfinishedSessionTag.getNext_token();
            logger.info("Running updates for tag {} and session_id {}", tag, session_id);
            String searchString = tag + " lang:it";
            String start_time = unixToTime(unfinishedSessionTag.getStart_time());
            String end_time = unixToTime(unfinishedSessionTag.getEnd_time());

            ArrayList<NameValuePair> queryParameters = new ArrayList<>();
            queryParameters.add(new BasicNameValuePair("query", searchString));
            queryParameters.add(new BasicNameValuePair("start_time", start_time));
            queryParameters.add(new BasicNameValuePair("end_time", end_time));

            Map<String, String> ids = new HashMap<>();
            ids.put("tag", tag);
            ids.put("session_id", session_id.toString());
            boolean result = twitterClient_v2.search(queryParameters, ids, next_token);

            if (result && tag != null) {
                sessionTagRepository.updateDone(tag, session_id);
                logger.info("Updated data for tag {} and session_id {}", tag, session_id);
            } else {
                logger.info("No db update needed");
            }
        }
        if (!hasBlockJobs) {
            logger.debug("No recent jobs");
        }

        List<Tag> oneTagToDo = tagRepository.getAllTagsToDo();
        String next_token;
        for (Tag tagToDo : oneTagToDo) {
            hasHistoricalJobs = true;
            String tag = tagToDo.getTag();
            long insert_time = tagToDo.getInsert_time();
            next_token = tagToDo.getNext_token();
            logger.info("Running historical updates for tag {} and insert_time {}", tag, insert_time);
            String searchString = tag + " lang:it";
            String start_time = unixToTime(tagToDo.getStart_time());
            String end_time = unixToTime(insert_time);

            ArrayList<NameValuePair> queryParameters = new ArrayList<>();
            queryParameters.add(new BasicNameValuePair("query", searchString));
            queryParameters.add(new BasicNameValuePair("start_time", start_time));
            queryParameters.add(new BasicNameValuePair("end_time", end_time));

            Map<String, String> ids = new HashMap<>();
            ids.put("tag", tag);
            ids.put("insert_time", Long.toString(insert_time));
            boolean result = twitterClient_v2.search(queryParameters, ids, next_token);

            if (result && tag != null) {
                tagRepository.setTagDone(tag, insert_time);
                logger.info("Updated historical data for tag {} and insert_time {}", tag, insert_time);
            } else {
                logger.info("No db update needed for historical data");
            }
        }
        if (!hasHistoricalJobs) {
            logger.debug("No historical jobs");
        }

        hasJobs.set(hasBlockJobs || hasHistoricalJobs);
    }
}
