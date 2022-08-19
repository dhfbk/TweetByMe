package eu.fbk.dh.twitter.runners;

import com.google.common.collect.Iterables;
import eu.fbk.dh.twitter.clients.TwitterClient_v2;
import eu.fbk.dh.twitter.tables.*;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static eu.fbk.dh.twitter.utils.Utils.unixToTime;

public class RecentUpdaterRunner {
    protected static final Logger logger = LogManager.getLogger();
    private static long TWITTER_START_TIME = 1262304000L;

    TwitterClient_v2 twitterClient_v2;

    SessionTagRepository sessionTagRepository;
    TagRepository tagRepository;
    ForeverTagRepository foreverTagRepository;
    int group_by;

    private AtomicBoolean hasJobs = new AtomicBoolean(true);

    public boolean hasJobs() {
        return hasJobs.get();
    }

    public RecentUpdaterRunner(TwitterClient_v2 twitterClient_v2, ForeverTagRepository foreverTagRepository,
                               SessionTagRepository sessionTagRepository, TagRepository tagRepository, int group_by) {
        this.twitterClient_v2 = twitterClient_v2;
        this.sessionTagRepository = sessionTagRepository;
        this.tagRepository = tagRepository;
        this.foreverTagRepository = foreverTagRepository;
        this.group_by = group_by;
    }

    public void update() throws IOException, URISyntaxException, InterruptedException {
        boolean hasBlockJobs = false;
        boolean hasHistoricalJobs = false;
        boolean hasForeverJobs = false;

        // To limit the number of queries to the Twitter API, in this code
        // the hashtags are grouped in sets by [group_by].
        // Keep the TreeSet, so that the order of tags is maintained.

        // lang -> interval -> tag
        Map<String, Map<String, Set<String>>> toDownload = new HashMap<>();

        List<SessionTag> unfinishedSessionTags = sessionTagRepository.getUnfinishedSessionTags();

        for (SessionTag unfinishedSessionTag : unfinishedSessionTags) {
            String lang = unfinishedSessionTag.getLang();
            toDownload.putIfAbsent(lang, new HashMap<>());
            String interval = unfinishedSessionTag.getStart_time() + "-"
                    + unfinishedSessionTag.getEnd_time() + "-"
                    + unfinishedSessionTag.getSession_id() + "-"
                    + unfinishedSessionTag.getNext_token();
            toDownload.get(lang).putIfAbsent(interval, new TreeSet<>());
            toDownload.get(lang).get(interval).add(unfinishedSessionTag.getTag());
        }

        for (String lang : toDownload.keySet()) {
            for (String key : toDownload.get(lang).keySet()) {
                hasBlockJobs = true;
                String[] parts = key.split("-", -1);
                String start_time = unixToTime(Long.parseLong(parts[0]));
                String end_time = unixToTime(Long.parseLong(parts[1]));
                String session_id = parts[2];
                String next_token = parts[3];
                Set<Set<String>> tagGroups = new HashSet<>();
                if (next_token.equals("")) {
                    Iterable<List<String>> partition = Iterables.partition(toDownload.get(lang).get(key), group_by);
                    for (List<String> strings : partition) {
                        tagGroups.add(new TreeSet<>(strings));
                    }
                } else {
                    tagGroups.add(new TreeSet<>(toDownload.get(lang).get(key)));
                }
                for (Set<String> tagList : tagGroups) {
                    String tags = String.join(" OR ", tagList);
                    String searchString = "(" + tags + ") lang:" + lang;
                    logger.info("Running updates for tags {} and session_id {}", tags, session_id);
                    ArrayList<NameValuePair> queryParameters = new ArrayList<>();
                    queryParameters.add(new BasicNameValuePair("query", searchString));
                    queryParameters.add(new BasicNameValuePair("start_time", start_time));
                    queryParameters.add(new BasicNameValuePair("end_time", end_time));

                    Map<String, String> ids = new HashMap<>();
                    ids.put("tags", String.join(" ", tagList));
                    ids.put("session_id", session_id);
                    ids.put("lang", lang);
                    boolean result = twitterClient_v2.search(queryParameters, ids, next_token);

                    if (result) {
                        for (String tag : tagList) {
                            sessionTagRepository.updateDone(tag, lang, Long.parseLong(session_id));
                            logger.info("Updated data for tag {} and session_id {}", tag, session_id);
                        }
                    } else {
                        logger.info("No db update needed");
                    }
                }

            }
        }

        if (!hasBlockJobs) {
            logger.debug("No recent jobs");
        }

        List<Tag> oneTagToDo = tagRepository.getAllTagsToDo();
        for (Tag tagToDo : oneTagToDo) {
            hasHistoricalJobs = true;
            String tag = tagToDo.getTag();
            String lang = tagToDo.getLang();
            long insert_time = tagToDo.getInsert_time();
            String next_token = tagToDo.getNext_token();
            logger.info("Running historical updates for tag {}, lang {}, and insert_time {}", tag, lang, insert_time);
            String searchString = tag + " lang:" + lang;
            String start_time = unixToTime(tagToDo.getStart_time());
            String end_time = unixToTime(insert_time);

            ArrayList<NameValuePair> queryParameters = new ArrayList<>();
            queryParameters.add(new BasicNameValuePair("query", searchString));
            queryParameters.add(new BasicNameValuePair("start_time", start_time));
            queryParameters.add(new BasicNameValuePair("end_time", end_time));

            Map<String, String> ids = new HashMap<>();
            ids.put("tag", tag);
            ids.put("insert_time", Long.toString(insert_time));
            ids.put("lang", lang);
            boolean result = twitterClient_v2.search(queryParameters, ids, next_token);

            if (result && tag != null) {
                tagRepository.setTagDone(tag, lang, insert_time);
                logger.info("Updated historical data for tag {} and insert_time {}", tag, insert_time);
            } else {
                logger.info("No db update needed for historical data");
            }
        }
        if (!hasHistoricalJobs) {
            logger.debug("No historical jobs");
        }

        List<ForeverTag> foreverTagToDo = foreverTagRepository.getAllTagsToDo();
        for (ForeverTag tagToDo : foreverTagToDo) {
            hasForeverJobs = true;
            String tag = tagToDo.getTag();
            String lang = tagToDo.getLang();
            long insert_time = tagToDo.getInsert_time();
            String next_token = tagToDo.getNext_token();
            if (next_token == null) {
                next_token = "";
            }
            logger.info("Running forever updates for tag {}, lang {}, and insert_time {}", tag, lang, insert_time);
            String searchString = tag + " lang:" + lang;
            String end_time = unixToTime(insert_time);
            Long start_time_long = tagToDo.getStart_time();
            if (start_time_long == null) {
                start_time_long = TWITTER_START_TIME;
            }
            if (start_time_long < TWITTER_START_TIME) {
                start_time_long = TWITTER_START_TIME;
            }
            String start_time = unixToTime(start_time_long);

            ArrayList<NameValuePair> queryParameters = new ArrayList<>();
            queryParameters.add(new BasicNameValuePair("query", searchString));
            queryParameters.add(new BasicNameValuePair("start_time", start_time));
            queryParameters.add(new BasicNameValuePair("end_time", end_time));

            Map<String, String> ids = new HashMap<>();
            ids.put("tag", tag);
            ids.put("forever_time", Long.toString(insert_time));
            ids.put("lang", lang);
            boolean result = twitterClient_v2.search(queryParameters, ids, next_token);

            if (result && tag != null) {
                foreverTagRepository.setTagDone(tag, lang, insert_time);
                logger.info("Updated forever data for tag {} and insert_time {}", tag, insert_time);
            } else {
                logger.info("No db update needed for forever data");
            }
        }
        if (!hasForeverJobs) {
            logger.debug("No forever jobs");
        }
        hasJobs.set(hasBlockJobs || hasHistoricalJobs || hasForeverJobs);
    }
}
