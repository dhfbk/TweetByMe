package eu.fbk.dh.twitter;

import com.google.common.collect.BiMap;
import com.google.common.collect.Sets;
import eu.fbk.dh.twitter.clients.GenericClient;
import eu.fbk.dh.twitter.clients.TwitterClient_v1;
import eu.fbk.dh.twitter.mongo.TweetRepository;
import eu.fbk.dh.twitter.runners.*;
import eu.fbk.dh.twitter.tables.*;
import eu.fbk.dh.twitter.utils.Defaults;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootApplication
public class Main implements CommandLineRunner {

    protected static final Logger logger = LogManager.getLogger();
    private static String FOREVER_PREFIX = "forever";

//    private static boolean RUN_TREND_SERVER = true;
//    private static boolean RUN_UPDATE_PREVIOUS = true;
//    private static boolean RUN_CRAWLER = true;

    private static Integer SLEEP_MS = 2000;
    private static Integer EMPTY_MS = 5000;

    HashMap<String, String> options = Defaults.getDefaultOptions();
    TwitterClient_v1 twitterClient_v1;
    GenericClient twitterConverter;
    GenericClient ppp = null;
    TwitterClientSave_v2 twitterClient_v2;

    private AtomicBoolean interrupt = new AtomicBoolean(false);

    RecentUpdaterRunner recentUpdaterRunner;
    Thread updateRecentThread;
    UpdateRecent updateRecent;
    ScheduledExecutorService trendsExecutor;
    ScheduledExecutorService optionsExecutor;

    Thread crawlerThread;

    @Autowired
    private TweetRepository tweetRepository;

    @Autowired
    private SessionRepository sessionRepository;
    @Autowired
    private SessionTagRepository sessionTagRepository;
    @Autowired
    private OptionRepository optionRepository;
    @Autowired
    private TagRepository tagRepository;
    @Autowired
    private HistoryTagRepository historyTagRepository;
    @Autowired
    private ForeverTagRepository foreverTagRepository;

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        loadOptions();
        String bearerToken = options.get("twitter.bearer");

        if (bearerToken == null || bearerToken.isEmpty()) {
            logger.error("Missing bearer token");
            System.exit(1);
        }

        twitterClient_v1 = new TwitterClient_v1(
                options.get("twitter.consumer_key"),
                options.get("twitter.consumer_secret"),
                options.get("twitter.access_token"),
                options.get("twitter.access_token_secret"));
        twitterConverter = new GenericClient(
                options.get("tweet.converter_host"),
                options.get("tweet.converter_port"),
                options.get("tweet.converter_protocol"),
                options.get("tweet.converter_address"));
        if (options.get("app.use_ppp").equals("1")) {
            ppp = new GenericClient(
                    options.get("tweet.ppp_host"),
                    options.get("tweet.ppp_port"),
                    options.get("tweet.ppp_protocol"),
                    options.get("tweet.ppp_address"));
        }

        twitterClient_v2 = new TwitterClientSave_v2(options, tweetRepository, twitterConverter, ppp,
                sessionRepository, tagRepository, sessionTagRepository, foreverTagRepository);

        if (options.get("app.empty_db").equals("1")) {
            logger.warn(String.format("Emptying database in %d seconds", EMPTY_MS / 1000));
            Thread.sleep(EMPTY_MS);

            optionRepository.saveOption("app.empty_db", "0");

            // Delete SQLite
            tagRepository.deleteAll();
            sessionRepository.deleteAll();
            sessionTagRepository.deleteAll();
            foreverTagRepository.deleteAll();
            historyTagRepository.deleteAll();

            // Delete mongo
            tweetRepository.deleteAll();

            // Delete rules
            twitterClient_v2.deleteAllRules();

            logger.info("Exiting");
            System.exit(1);
        }

        if (options.get("twitter.empty_rules").equals("1")) {
            logger.warn(String.format("Deleting Twitter rules in %d seconds", EMPTY_MS / 1000));
            Thread.sleep(EMPTY_MS);
            optionRepository.saveOption("twitter.empty_rules", "0");
            twitterClient_v2.deleteAllRules();
        }

        // todo: execute this at fixed rate
        BiMap<String, String> foreverRules = twitterClient_v2.getRules(FOREVER_PREFIX);
        Set<String> initialHashtags = new HashSet<>(foreverRules.values());

        List<ForeverTag> foreverTags = foreverTagRepository.getTags();
        Set<String> finalHashtags = new HashSet<>();
        for (ForeverTag unexpiredTag : foreverTags) {
            String tag = unexpiredTag.getTag();
            String lang = unexpiredTag.getLang();
            finalHashtags.add(tag + " lang:" + lang);
        }

        Sets.SetView<String> toDelete = Sets.difference(initialHashtags, finalHashtags);
        Sets.SetView<String> toAdd = Sets.difference(finalHashtags, initialHashtags);

        List<String> idsToDelete = new ArrayList<>();
        List<String> tagsToDelete = new ArrayList<>();
        for (String tag : toDelete) {
            String id = foreverRules.inverse().get(tag);
            idsToDelete.add(id);
            tagsToDelete.add(tag);
        }

        logger.debug("Rules to delete: {}", tagsToDelete.toString());
        twitterClient_v2.deleteRules(idsToDelete);
        logger.debug("Rules to add: {}", toAdd.toString());
        twitterClient_v2.createRules(toAdd, FOREVER_PREFIX);

        recentUpdaterRunner = new RecentUpdaterRunner(twitterClient_v2, foreverTagRepository, sessionTagRepository, tagRepository, Integer.parseInt(options.get("tweet.group_by")));

        Crawler tweetsCrawler = new Crawler(twitterClient_v2, recentUpdaterRunner, sessionRepository, sessionTagRepository, tagRepository, foreverTagRepository);

        AtomicBoolean trendsClientRunning = new AtomicBoolean(false);
        TrendsDownloader trendsDownloader = new TrendsDownloader(twitterClient_v1, twitterClient_v2, options, tagRepository, trendsClientRunning, recentUpdaterRunner, historyTagRepository);

        if (options.get("app.run_trend_server").equals("1")) {
            logger.info("RUNNING trend launcher");
            trendsExecutor = Executors.newScheduledThreadPool(2, new namedThreadFactory("trends-executor"));
            trendsExecutor.scheduleAtFixedRate(trendsDownloader, 0L, Long.parseLong(options.get("tweet.trends_update_interval_minutes")), TimeUnit.MINUTES);
        } else {
            logger.info("**NOT** RUNNING trend launcher");
        }

        optionsExecutor = Executors.newScheduledThreadPool(2, new namedThreadFactory("trends-executor"));
        optionsExecutor.scheduleAtFixedRate(this::loadOptions, 0L, Long.parseLong(options.get("tweet.options_update_interval_minutes")), TimeUnit.MINUTES);
        long closeInterval = Long.parseLong(options.get("tweet.options_close_stream_minutes"));
        if (closeInterval > 0) {
            logger.info("RUNNING closer");
            optionsExecutor.scheduleAtFixedRate(this::closeStream, closeInterval, closeInterval, TimeUnit.MINUTES);
        } else {
            logger.info("**NOT** RUNNING closer");
        }

        crawlerThread = new Thread(tweetsCrawler, "tweets-crawler");
        if (options.get("app.run_crawler").equals("1")) {
            logger.info("RUNNING crawler");
            crawlerThread.start();
        } else {
            logger.info("**NOT** RUNNING crawler");
        }

        updateRecent = new UpdateRecent(recentUpdaterRunner);
        updateRecentThread = new Thread(updateRecent, "update-recent");
        if (options.get("app.run_update_previous").equals("1")) {
            logger.info("RUNNING recent tweets updater");
            updateRecentThread.start();
        } else {
            logger.info("**NOT** RUNNING recent tweets updater");
        }

        while (true) {
            try {
                if (interrupt.get()) {
                    break;
                }
                logger.trace("Loop");
                Thread.sleep(SLEEP_MS);
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private void closeStream() {
        logger.info("Closing stream");
        twitterClient_v2.close();
    }

    private void loadOptions() {
        logger.info("Updating options");
        for (Option option : optionRepository.findAll()) {
            options.put(option.getId(), option.getValue());
        }
    }

}
