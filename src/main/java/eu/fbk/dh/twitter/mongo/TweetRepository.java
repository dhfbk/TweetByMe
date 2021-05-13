package eu.fbk.dh.twitter.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import java.util.List;

public interface TweetRepository extends MongoRepository<Tweet, Long> {

//    public Customer findByFirstName(String firstName);
//    public List<Customer> findByLastName(String lastName);

    @Query("{'created_at_ts': {$gte: ?0, $lte:?1}, 'entities.hashtags.text': ?2}")
    List<Tweet> getTweetsByIntervalAndTag(Long from, Long to, String tag);

    @Query("{'created_at_ts': {$gte: ?0, $lte:?1}}")
    List<Tweet> getTweetsByInterval(Long from, Long to);

    @Query("{'entities.hashtags.text': ?0}")
    List<Tweet> getTweetsByTag(String tag);

}
