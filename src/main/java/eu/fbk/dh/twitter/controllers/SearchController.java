package eu.fbk.dh.twitter.controllers;

import eu.fbk.dh.twitter.mongo.Hashtag;
import eu.fbk.dh.twitter.mongo.Tweet;
import eu.fbk.dh.twitter.mongo.TweetRepository;
import eu.fbk.dh.twitter.tables.HistoryTag;
import eu.fbk.dh.twitter.tables.HistoryTagRepository;
import eu.fbk.dh.twitter.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.time.YearMonth;
import java.util.*;

@RestController
public class SearchController {

    private final TweetRepository tweetRepository;
    private final HistoryTagRepository historyTagRepository;
    protected static final Logger logger = LogManager.getLogger();

    TimeZone timeZone = TimeZone.getTimeZone("Europe/Rome");

    public SearchController(TweetRepository tweetRepository, HistoryTagRepository historyTagRepository) {
        this.tweetRepository = tweetRepository;
        this.historyTagRepository = historyTagRepository;
    }

    Map<String, Map<String, Integer>> getMap(List<Tweet> tweets) {
        return getMap(tweets, 10);
    }

    Map<String, Map<String, Integer>> getMap(List<Tweet> tweets, Integer split) {
        return getMap(tweets, split, null);
    }

    Map<String, Map<String, Integer>> getMap(List<Tweet> tweets, Integer split, Set<String> splitByTag) {

        Map<String, Map<String, Integer>> retData = new HashMap<>();

        TreeMap<String, Integer> total = new TreeMap<>();

        TreeMap<String, Integer> pos = new TreeMap<>();
        TreeMap<String, Integer> neg = new TreeMap<>();

        TreeMap<String, Integer> surprise = new TreeMap<>();
        TreeMap<String, Integer> anger = new TreeMap<>();
        TreeMap<String, Integer> joy = new TreeMap<>();
        TreeMap<String, Integer> trust = new TreeMap<>();
        TreeMap<String, Integer> fear = new TreeMap<>();
        TreeMap<String, Integer> sadness = new TreeMap<>();
        TreeMap<String, Integer> anticipation = new TreeMap<>();
        TreeMap<String, Integer> disgust = new TreeMap<>();

        for (Tweet tweet : tweets) {
            String stringDateRaw = Utils.unixToTime(tweet.getCreated_at_ts(), timeZone).substring(0, split);

            List<String> strings = new ArrayList<>();
            if (splitByTag != null && splitByTag.size() > 0) {
                if (tweet.getEntities().getHashtags() != null) {
                    for (Hashtag hashtag : tweet.getEntities().getHashtags()) {
                        if (splitByTag.contains("#" + hashtag.getText())) {
                            strings.add(stringDateRaw + "-" + hashtag.getText());
                        }
                    }
                }
            } else {
                strings.add(stringDateRaw);
            }

            for (String key : strings) {
                pos.putIfAbsent(key, 0);
                neg.putIfAbsent(key, 0);
                surprise.putIfAbsent(key, 0);
                anger.putIfAbsent(key, 0);
                joy.putIfAbsent(key, 0);
                trust.putIfAbsent(key, 0);
                fear.putIfAbsent(key, 0);
                sadness.putIfAbsent(key, 0);
                anticipation.putIfAbsent(key, 0);
                disgust.putIfAbsent(key, 0);
                total.putIfAbsent(key, 0);

                pos.put(key, pos.get(key) + tweet.getPositive());
                neg.put(key, neg.get(key) + tweet.getNegative());
                surprise.put(key, anger.get(key) + tweet.getEmotions().getEmotions().getNrcDict_Surprise());
                anger.put(key, surprise.get(key) + tweet.getEmotions().getEmotions().getNrcDict_Anger());
                joy.put(key, joy.get(key) + tweet.getEmotions().getEmotions().getNrcDict_Joy());
                trust.put(key, trust.get(key) + tweet.getEmotions().getEmotions().getNrcDict_Trust());
                fear.put(key, fear.get(key) + tweet.getEmotions().getEmotions().getNrcDict_Fear());
                sadness.put(key, sadness.get(key) + tweet.getEmotions().getEmotions().getNrcDict_Sadness());
                anticipation.put(key, anticipation.get(key) + tweet.getEmotions().getEmotions().getNrcDict_Anticipation());
                disgust.put(key, disgust.get(key) + tweet.getEmotions().getEmotions().getNrcDict_Disgust());
                total.put(key, total.get(key) + 1);
            }

        }

        retData.put("positive", pos);
        retData.put("negative", neg);
        retData.put("surprise", surprise);
        retData.put("anger", anger);
        retData.put("joy", joy);
        retData.put("trust", trust);
        retData.put("fear", fear);
        retData.put("sadness", sadness);
        retData.put("anticipation", anticipation);
        retData.put("disgust", disgust);

        retData.put("total", total);

        return retData;
    }

    @GetMapping("/tweets/{id}")
    Tweet one(@PathVariable Long id) {
        return tweetRepository.findById(id).orElseThrow(() -> new TweetNotFoundException(id));
    }

    @GetMapping("/tweets/tag/{tag}")
    Map<String, Map<String, Integer>> searchByTag(@PathVariable String tag) {
        return getMap(tweetRepository.getTweetsByTag(tag));
    }

    @GetMapping("/tweets/timetag/{year}/{month}/{tag}")
    Map<String, Map<String, Integer>> search(@PathVariable Integer year, @PathVariable Integer month, @PathVariable String tag) {

        YearMonth yearMonthObject = YearMonth.of(year, month);
        int daysInMonth = yearMonthObject.lengthOfMonth();

        Long startTime = Utils.timeToUnix(year, month, 1, 0, 0, 0, timeZone);
        Long endTime = Utils.timeToUnix(year, month, daysInMonth, 23, 59, 59, timeZone);
        List<Tweet> tweets = tweetRepository.getTweetsByIntervalAndTag(startTime, endTime, tag);

        return getMap(tweets);
    }

    @GetMapping("/tweets/timetag/{year}/{month}/{day}/{tag}")
    Map<String, Map<String, Integer>> search(@PathVariable Integer year, @PathVariable Integer month, @PathVariable Integer day, @PathVariable String tag, boolean splitByHours) {

        Long startTime = Utils.timeToUnix(year, month, day, 0, 0, 0, timeZone);
        Long endTime = Utils.timeToUnix(year, month, day, 23, 59, 59, timeZone);
        List<Tweet> tweets = tweetRepository.getTweetsByIntervalAndTag(startTime, endTime, tag);

        int split = 10;
        if (splitByHours) {
            split = 13;
        }

        return getMap(tweets, split);
    }

    @GetMapping("/tweets/time/{year}/{month}/{day}")
    Map<String, Map<String, Integer>> search(@PathVariable Integer year, @PathVariable Integer month, @PathVariable Integer day, boolean splitByHours, boolean splitByTags) {

        Long startTime = Utils.timeToUnix(year, month, day, 0, 0, 0, timeZone);
        Long endTime = Utils.timeToUnix(year, month, day, 23, 59, 59, timeZone);
        List<HistoryTag> tags = historyTagRepository.getTagsWithInterval(startTime, endTime);
        Set<String> uniqueTags = null;
        if (splitByTags) {
            uniqueTags = new HashSet<>();
            for (HistoryTag tag : tags) {
                uniqueTags.add(tag.getTag());
            }
        }

        int split = 10;
        if (splitByHours) {
            split = 13;
        }

        List<Tweet> tweets = tweetRepository.getTweetsByInterval(startTime, endTime);

        return getMap(tweets, split, uniqueTags);
    }

}
