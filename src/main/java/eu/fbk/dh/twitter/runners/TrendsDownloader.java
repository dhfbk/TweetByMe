package eu.fbk.dh.twitter.runners;

import com.google.common.collect.BiMap;
import com.google.common.collect.Sets;
import eu.fbk.dh.twitter.clients.TwitterClient_v1;
import eu.fbk.dh.twitter.clients.TwitterClient_v2;
import eu.fbk.dh.twitter.tables.HistoryTag;
import eu.fbk.dh.twitter.tables.HistoryTagRepository;
import eu.fbk.dh.twitter.tables.Tag;
import eu.fbk.dh.twitter.tables.TagRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import twitter4j.Trend;
import twitter4j.Trends;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class TrendsDownloader implements Runnable {

    protected static final Logger logger = LogManager.getLogger();
    private static String TRENDS_PREFIX = "trend";

    TwitterClient_v1 twitterClient_v1;
    TwitterClient_v2 twitterClient_v2;
    HashMap<String, String> options;
    AtomicBoolean isRunning;
    final RecentUpdaterRunner recentUpdaterRunner;
    HistoryTagRepository historyTagRepository;

    TagRepository tagRepository;

    public TrendsDownloader(TwitterClient_v1 twitterClient_v1, TwitterClient_v2 twitterClient_v2,
                            HashMap<String, String> options,
                            TagRepository tagRepository,
                            AtomicBoolean isRunning, RecentUpdaterRunner recentUpdaterRunner, HistoryTagRepository historyTagRepository) {
        this.twitterClient_v1 = twitterClient_v1;
        this.twitterClient_v2 = twitterClient_v2;
        this.options = options;
        this.isRunning = isRunning;
        this.recentUpdaterRunner = recentUpdaterRunner;
        this.tagRepository = tagRepository;
        this.historyTagRepository = historyTagRepository;
    }

    @Override
    public void run() {
        if (isRunning.get()) {
            logger.warn("Trends client already running");
            return;
        }
        try {
            logger.info("Running trends client");
            isRunning.set(true);
            Trends trends = twitterClient_v1.getTwitter().getPlaceTrends(Integer.parseInt(options.get("tweet.trends_location")));
            Trend[] trendsTrends = trends.getTrends();

            long now = System.currentTimeMillis() / 1000L;

            BiMap<String, String> rules = twitterClient_v2.getRules(TRENDS_PREFIX);
            Set<String> initialHashtags = new HashSet<>(rules.values());
            logger.debug("Rules: {}", initialHashtags.toString());

            // Delete all rules
//            twitterClient_v2.deleteRules(new ArrayList<>(rules.keySet()));
//            initialHashtags = new HashSet<>();

            boolean notification = false;

            int j = 0;
            for (Trend trendsTrend : trendsTrends) {
                boolean hashtagOnly = Boolean.parseBoolean(options.get("tweet.trends_hashtag_only"));
                String trend = trendsTrend.getName().toLowerCase();
                if (hashtagOnly && trend.charAt(0) != '#') {
                    continue;
                }
                boolean update = j < Integer.parseInt(options.get("tweet.trends_number_update"));
                if (j++ >= Integer.parseInt(options.get("tweet.trends_number"))) {
                    break;
                }

                long expired_time = now + Long.parseLong(options.get("tweet.collect_next_days")) * 24L * 60L * 60L;

                List<Tag> unexpiredTagsByTag = tagRepository.getUnexpiredTagsByTag(trend, now);
                boolean present = unexpiredTagsByTag.size() > 0;

                HistoryTag historyTag = new HistoryTag();
                historyTag.setTag(trend);
                historyTag.setSession_id(now);
                historyTagRepository.save(historyTag);

                if (present && update) {
                    tagRepository.updateExpiredTime(expired_time, trend, now);
                }
                if (!present) {
                    long start_time = now - Long.parseLong(options.get("tweet.collect_previous_days")) * 24L * 60L * 60L;
                    Tag tag = new Tag();
                    tag.setTag(trend);
                    tag.setInsert_time(now);
                    tag.setStart_time(start_time);
                    tag.setExpired_time(expired_time);
                    tagRepository.save(tag);
                    notification = true;
                }
            }

            if (notification) {
                synchronized (recentUpdaterRunner) {
                    logger.info("Notification to UpdateRecent for trends");
                    recentUpdaterRunner.notify();
                }
            }

            List<Tag> unexpiredTags = tagRepository.getUnexpiredTags(now);
            Set<String> finalHashtags = new HashSet<>();
            for (Tag unexpiredTag : unexpiredTags) {
                String tag = unexpiredTag.getTag();
                finalHashtags.add(tag + " lang:it");
            }

            logger.trace("Initial rules: {}", initialHashtags);
            logger.trace("Final rules: {}", finalHashtags);

            Sets.SetView<String> toDelete = Sets.difference(initialHashtags, finalHashtags);
            Sets.SetView<String> toAdd = Sets.difference(finalHashtags, initialHashtags);

            List<String> idsToDelete = new ArrayList<>();
            for (String tag : toDelete) {
                String id = rules.inverse().get(tag);
                idsToDelete.add(id);
            }

            logger.debug("Rules to delete: {}", idsToDelete.toString());
            twitterClient_v2.deleteRules(idsToDelete);
            logger.debug("Rules to add: {}", toAdd.toString());
            twitterClient_v2.createRules(toAdd, TRENDS_PREFIX);

            logger.info("Ending trends client");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            isRunning.set(false);
        }
    }
}
