package eu.fbk.dh.twitter.controllers;

public class TweetNotFoundException extends RuntimeException {
    public TweetNotFoundException(Long id) {
        super("Could not find employee " + id);
    }
}
