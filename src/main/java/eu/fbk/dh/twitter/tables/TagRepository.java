package eu.fbk.dh.twitter.tables;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import javax.transaction.Transactional;
import java.util.List;

public interface TagRepository extends CrudRepository<Tag, Long> {

    @Query("FROM Tag WHERE start_time < :start_time AND expired_time > :expired_time")
    List<Tag> getTagsWithInterval(Long start_time, Long expired_time);

    @Query("FROM Tag WHERE done = 0 ORDER BY insert_time")
    List<Tag> getAllTagsToDo();

    @Query("FROM Tag WHERE tag = :tag AND expired_time > :expired_time")
    List<Tag> getUnexpiredTagsByTag(String tag, Long expired_time);

    @Query("FROM Tag WHERE expired_time > :expired_time")
    List<Tag> getUnexpiredTags(Long expired_time);

    @Transactional
    @Modifying
    @Query("UPDATE Tag SET done = 1, next_token = '' WHERE tag = :tag AND lang = :lang AND insert_time = :insert_time")
    void setTagDone(String tag, String lang, Long insert_time);

    @Transactional
    @Modifying
    @Query("UPDATE Tag SET expired_time = :new_expired_time WHERE tag = :tag AND lang = :lang AND expired_time > :expired_time")
    void updateExpiredTime(Long new_expired_time, String tag, String lang, Long expired_time);

    @Transactional
    @Modifying
    @Query("UPDATE Tag SET next_token = :next_token WHERE tag = :tag AND lang = :lang AND insert_time = :insert_time")
    void updateNextToken(String next_token, String tag, String lang, Long insert_time);

}
