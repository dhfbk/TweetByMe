package eu.fbk.dh.twitter.mongo;

import lombok.Data;

import javax.persistence.Id;
import java.util.List;
import java.util.Map;

@Data
public class Tweet {

    @Id
    Long id;
    String id_str;

    String date;
    Integer like_count;
    Emotions emotions;
    String created_at;
    Long session_id;
    String source;
    Integer reply_count;
    Map<String, Object> retweeted_status;
    Map<String, Object> replied_to;
    Map<String, Object> quoted_status;
    Integer retweet_count;
    Long created_at_ts;
    Entities entities;
    String conversation_id;
    String text;
    String lang;
    Map<String, Object> user;
    Integer quote_count;
    List<Object> matching_rules;

    public Integer getPositive() {
        EmotionsValues emotions = this.emotions.getEmotions();
        return emotions.getNrcDict_Surprise() + emotions.getNrcDict_Joy() + emotions.getNrcDict_Trust() + emotions.getNrcDict_Anticipation();
    }

    public Integer getNegative() {
        EmotionsValues emotions = this.emotions.getEmotions();
        return emotions.getNrcDict_Anger() + emotions.getNrcDict_Fear() + emotions.getNrcDict_Sadness() + emotions.getNrcDict_Disgust();
    }
}
