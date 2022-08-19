package eu.fbk.dh.twitter.mongo;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class Entities {
    List<Hashtag> hashtags;
    Map<String, Object> media;
    Map<String, Map<String, Object>> user_mentions;
    Map<String, Map<String, Object>> annotations;
    List<Map<String, Object>> url;
}
