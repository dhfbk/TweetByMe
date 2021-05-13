package eu.fbk.dh.twitter.runners;

import eu.fbk.dh.twitter.TwitterClientSave_v2;
import eu.fbk.dh.twitter.tables.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class Crawler implements Runnable {

    TwitterClientSave_v2 twitterClient_v2;
    final RecentUpdaterRunner recentUpdaterRunner;

    private SessionRepository sessionRepository;
    private SessionTagRepository sessionTagRepository;
    private TagRepository tagRepository;

    protected static final Logger logger = LogManager.getLogger();
    private static Integer SLEEP_MS = 1000;

    public Crawler(TwitterClientSave_v2 twitterClient_v2, RecentUpdaterRunner recentUpdaterRunner,
                   SessionRepository sessionRepository, SessionTagRepository sessionTagRepository,
                   TagRepository tagRepository) {
        this.twitterClient_v2 = twitterClient_v2;
        this.recentUpdaterRunner = recentUpdaterRunner;
        this.sessionRepository = sessionRepository;
        this.sessionTagRepository = sessionTagRepository;
        this.tagRepository = tagRepository;
    }

    @Override
    public void run() {
        while (true) {
            try {

                long session_id = System.currentTimeMillis() / 1000L;
                logger.info("Starting session {}", session_id);
                try {
                    List<Session> lastSession = sessionRepository.getSessionsSortedByTimeDesc();
                    long start_time = session_id;
                    if (lastSession.size() == 1) {
                        start_time = lastSession.get(0).getEnd_time();
                    }

                    Session session = new Session();
                    session.setSession_id(session_id);
                    session.setStart_time(start_time);
                    session.setEnd_time(session_id);
                    sessionRepository.save(session);

                    boolean notification = false;

                    List<Session> unfinishedSessions = sessionRepository.getUnfinishedSessions(session_id);
                    for (Session unfinishedSession : unfinishedSessions) {
                        Long old_start_time = unfinishedSession.getStart_time();
                        Long old_session_id = unfinishedSession.getSession_id();

                        List<SessionTag> unfinishedSessionTags = sessionTagRepository.getSessionTagsPerSession(old_session_id);
                        if (unfinishedSessionTags.size() > 0) {
                            continue;
                        }

                        List<Tag> tagsWithInterval = tagRepository.getTagsWithInterval(old_start_time, old_start_time);
                        for (Tag tag : tagsWithInterval) {
                            SessionTag sessionTag = new SessionTag();
                            sessionTag.setTag(tag.getTag());
                            sessionTag.setSession_id(old_session_id);
                            sessionTag.setStart_time(old_start_time);
                            sessionTag.setEnd_time(old_session_id);
                            sessionTagRepository.save(sessionTag);
                            notification = true;
                        }
                    }

                    sessionRepository.setSessionAsDone(session_id);

                    if (notification) {
                        synchronized (recentUpdaterRunner) {
                            logger.info("Notification to UpdateRecent for session");
                            recentUpdaterRunner.notify();
                        }
                    }

                } catch (Exception e) {
                    logger.error(e.getMessage());
                }

                twitterClient_v2.setSession_id(session_id);
                twitterClient_v2.connectStream();
                logger.warn("Stream ended");
            } catch (Exception e) {
                if (twitterClient_v2.isClosed.get()) {
                    Thread.currentThread().interrupt();
                    break;
                } else {
                    logger.error(e.getMessage());
                    logger.info("Restarting service");
                    try {
                        Thread.sleep(SLEEP_MS);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                        break;
                    }
                }
            }
        }
    }
}
