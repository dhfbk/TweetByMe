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

        // Sleep 20 seconds to avoid Twitter error:
        // 'end_time' must be a minimum of 10 seconds prior to the request time.
        try {
            logger.info("UpdateRecent sleeping");
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            return;
        }

        while (true) {
            if (Thread.currentThread().isInterrupted() || isClosed.get()) {
                break;
            }
            try {
                recentUpdaterRunner.update();
//                if (true) {
                if (!recentUpdaterRunner.hasJobs()) {
                    synchronized (recentUpdaterRunner) {
                        logger.info("No more jobs to do, waiting");
                        try {
                            recentUpdaterRunner.wait();

                            // Same as above: sleep 15 seconds to avoid Twitter error:
                            // 'end_time' must be a minimum of 10 seconds prior to the request time.
                            logger.info("New tasks coming, starting soon");
                            Thread.sleep(15000);
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
