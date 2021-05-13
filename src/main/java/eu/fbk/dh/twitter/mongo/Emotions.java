package eu.fbk.dh.twitter.mongo;

import lombok.Data;

@Data
public class Emotions {
    String preprocessEmotions;
    EmotionsValues emotions;
    String preprocessedText;
}
