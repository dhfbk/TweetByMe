package eu.fbk.dh.twitter;

import eu.fbk.dh.twitter.clients.GenericClient;
import eu.fbk.dh.twitter.clients.TwitterClient_v1;
import eu.fbk.dh.twitter.mongo.TweetRepository;
import eu.fbk.dh.twitter.runners.Crawler;
import eu.fbk.dh.twitter.runners.RecentUpdaterRunner;
import eu.fbk.dh.twitter.runners.TrendsDownloader;
import eu.fbk.dh.twitter.runners.UpdateRecent;
import eu.fbk.dh.twitter.tables.*;
import eu.fbk.dh.twitter.utils.Defaults;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.core.MongoOperations;

import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootApplication
public class Main implements CommandLineRunner {

    protected static final Logger logger = LogManager.getLogger();

    private static boolean RUN_TREND_SERVER = true;
    private static boolean RUN_UPDATE_PREVIOUS = true;
    private static boolean RUN_CRAWLER = true;

    private static Integer SLEEP_MS = 2000;

    HashMap<String, String> options = Defaults.getDefaultOptions();
    TwitterClient_v1 twitterClient_v1;
    GenericClient twitterConverter;
    GenericClient ppp;
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

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        loadOptions();

        if (options.get("app.empty_db").equals("1")) {
            Option option = new Option();
            option.setId("app.empty_db");
            option.setValue("0");
            optionRepository.save(option);
            tagRepository.deleteAll();
            sessionRepository.deleteAll();
            sessionTagRepository.deleteAll();
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
        ppp = new GenericClient(
                options.get("tweet.ppp_host"),
                options.get("tweet.ppp_port"),
                options.get("tweet.ppp_protocol"),
                options.get("tweet.ppp_address"));

        String bearerToken = options.get("twitter.bearer");
//        if (bearerToken == null || bearerToken.length() == 0) {
//            logger.error("Twitter bearer token is null, exiting...");
//            this.setInterrupt();
//            return;
//        }
        twitterClient_v2 = new TwitterClientSave_v2(bearerToken, tweetRepository, twitterConverter, ppp,
                Long.parseLong(options.get("tweet.alive_interval_minutes")) * 60,
                sessionRepository, tagRepository, sessionTagRepository);

//        if (interrupt.get()) {
//            return;
//        }

        recentUpdaterRunner = new RecentUpdaterRunner(twitterClient_v2, sessionTagRepository, tagRepository);

        Crawler tweetsCrawler = new Crawler(twitterClient_v2, recentUpdaterRunner, sessionRepository, sessionTagRepository, tagRepository);

        AtomicBoolean trendsClientRunning = new AtomicBoolean(false);
        TrendsDownloader trendsDownloader = new TrendsDownloader(twitterClient_v1, twitterClient_v2, options, tagRepository, trendsClientRunning, recentUpdaterRunner, historyTagRepository);

        trendsExecutor = Executors.newScheduledThreadPool(2);
        if (RUN_TREND_SERVER) {
            trendsExecutor.scheduleAtFixedRate(trendsDownloader, 0L, Long.parseLong(options.get("tweet.trends_update_interval_minutes")), TimeUnit.MINUTES);
        }
        trendsExecutor.scheduleAtFixedRate(this::loadOptions, 0L, Long.parseLong(options.get("tweet.options_update_interval_minutes")), TimeUnit.MINUTES);

        crawlerThread = new Thread(tweetsCrawler);
        if (RUN_CRAWLER) {
            crawlerThread.start();
        }

        updateRecent = new UpdateRecent(recentUpdaterRunner);
        updateRecentThread = new Thread(updateRecent);
        if (RUN_UPDATE_PREVIOUS) {
            updateRecentThread.start();
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
