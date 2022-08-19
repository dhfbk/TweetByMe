package eu.fbk.dh.twitter;

import com.google.common.collect.BiMap;
import com.google.common.collect.Sets;
import eu.fbk.dh.twitter.clients.GenericClient;
import eu.fbk.dh.twitter.clients.TwitterClient_v1;
import eu.fbk.dh.twitter.mongo.TweetRepository;
import eu.fbk.dh.twitter.runners.*;
import eu.fbk.dh.twitter.tables.*;
import eu.fbk.dh.twitter.utils.Defaults;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

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

        twitterClient_v2 = new TwitterClientSave_v2(bearerToken, tweetRepository, twitterConverter, ppp,
                Long.parseLong(options.get("tweet.alive_interval_minutes")) * 60,
                sessionRepository, tagRepository, sessionTagRepository, foreverTagRepository);

        if (options.get("app.empty_db").equals("1")) {
            logger.warn(String.format("Emptying database in %d seconds", EMPTY_MS / 1000));
            Thread.sleep(EMPTY_MS);

            Option option = new Option();
            option.setId("app.empty_db");
            option.setValue("0");
            optionRepository.save(option);

            // Delete SQLite
            tagRepository.deleteAll();
            sessionRepository.deleteAll();
            sessionTagRepository.deleteAll();
            foreverTagRepository.deleteAll();
            historyTagRepository.deleteAll();

            // Delete mongo
            tweetRepository.deleteAll();

            // Delete rules
            BiMap<String, String> rules = twitterClient_v2.getRules(null);
            List<String> rulesToDelete = new ArrayList<>(rules.keySet());
            twitterClient_v2.deleteRules(rulesToDelete);
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

        trendsExecutor = Executors.newScheduledThreadPool(2, new namedThreadFactory("trends-executor"));
        if (options.get("app.run_trend_server").equals("1")) {
            logger.info("RUNNING trend launcher");
            trendsExecutor.scheduleAtFixedRate(trendsDownloader, 0L, Long.parseLong(options.get("tweet.trends_update_interval_minutes")), TimeUnit.MINUTES);
        }
        else {
            logger.info("**NOT** RUNNING trend launcher");
        }

        trendsExecutor.scheduleAtFixedRate(this::loadOptions, 0L, Long.parseLong(options.get("tweet.options_update_interval_minutes")), TimeUnit.MINUTES);

        crawlerThread = new Thread(tweetsCrawler, "tweets-crawler");
        if (options.get("app.run_crawler").equals("1")) {
            logger.info("RUNNING crawler");
            crawlerThread.start();
        }
        else {
            logger.info("**NOT** RUNNING crawler");
        }

        updateRecent = new UpdateRecent(recentUpdaterRunner);
        updateRecentThread = new Thread(updateRecent, "update-recent");
        if (options.get("app.run_update_previous").equals("1")) {
            logger.info("RUNNING recent tweets updater");
            updateRecentThread.start();
        }
        else {
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


//        MongoOperations mongoOps = new MongoTemplate(MongoClients.create(), "database");
//        ObjectMapper objectMapper = new ObjectMapper();
//        Tweet tweet = objectMapper.readValue(json, Tweet.class);
//        tweetRepository.save(tweet);
//        mongoOps.getCollection("pippo").insertOne(Document.parse(json));

//        customerRepository.deleteAll();

//        long now = System.currentTimeMillis() / 1000L;
//        Session session = new Session();
//        session.setSession_id(now);
//        session.setStart_time(now + 1);
//        session.setEnd_time(now + 2);
//        sessionRepository.save(session);

        // save a couple of customers
//        customerRepository.save(new Customer("Alice", "Smith"));
//        customerRepository.save(new Customer("Bob", "Smith"));

        // fetch all customers
//        logger.info("Customers found with findAll():");
//        logger.info("-------------------------------");
//        for (Customer customer : customerRepository.findAll()) {
//            logger.info(customer);
//        }

//        // fetch an individual customer
//        System.out.println("Customer found with findByFirstName('Alice'):");
//        System.out.println("--------------------------------");
//        System.out.println(customerRepository.findByFirstName("Alice"));
//
//        System.out.println("Customers found with findByLastName('Smith'):");
//        System.out.println("--------------------------------");
//        for (Customer customer : customerRepository.findByLastName("Smith")) {
//            System.out.println(customer);
//        }

    }

    private void loadOptions() {
        for (Option option : optionRepository.findAll()) {
            options.put(option.getId(), option.getValue());
        }
    }

}
