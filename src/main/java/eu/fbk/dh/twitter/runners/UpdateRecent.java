package eu.fbk.dh.twitter.runners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public class UpdateRecent implements Runnable {
    protected static final Logger logger = LogManager.getLogger();

    private final RecentUpdaterRunner recentUpdaterRunner;
    private AtomicBoolean isClosed = new AtomicBoolean(false);

    public UpdateRecent(RecentUpdaterRunner recentUpdaterRunner) {
        this.recentUpdaterRunner = recentUpdaterRunner;
    }

    public void close() {
        isClosed.set(true);
        Thread.currentThread().interrupt();
    }

    @Override
    public void run() {
        while (true) {
            if (Thread.currentThread().isInterrupted() || isClosed.get()) {
                break;
            }
            try {
                recentUpdaterRunner.update();
                if (!recentUpdaterRunner.hasJobs()) {
                    synchronized (recentUpdaterRunner) {
                        logger.info("No more jobs to do, waiting");
                        try {
                            recentUpdaterRunner.wait();
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                if (!isClosed.get()) {
                    logger.error(e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }
}
