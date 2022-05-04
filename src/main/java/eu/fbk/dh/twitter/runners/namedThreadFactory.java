package eu.fbk.dh.twitter.runners;

import java.util.concurrent.ThreadFactory;

public class namedThreadFactory implements ThreadFactory {
    String name;

    public namedThreadFactory(String name) {
        this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, name);
    }
}
