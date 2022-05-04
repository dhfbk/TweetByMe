package eu.fbk.dh.twitter.tables;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import javax.transaction.Transactional;
import java.util.List;

public interface ForeverTagRepository extends CrudRepository<ForeverTag, Long> {

    @Query("FROM ForeverTag WHERE active = 1")
    List<ForeverTag> getTags();

    @Query("FROM ForeverTag WHERE start_time IS NULL OR start_time < :start_time AND active = 1")
    List<ForeverTag> getTagsWithInterval(Long start_time);

    @Query("FROM ForeverTag WHERE done = 0 AND active = 1 ORDER BY insert_time")
    List<ForeverTag> getAllTagsToDo();

    @Transactional
    @Modifying
    @Query("UPDATE ForeverTag SET done = 1, next_token = '' WHERE tag = :tag AND insert_time = :insert_time")
    void setTagDone(String tag, Long insert_time);

    @Transactional
    @Modifying
    @Query("UPDATE ForeverTag SET next_token = :next_token WHERE tag = :tag AND insert_time = :insert_time")
    void updateNextToken(String next_token, String tag, Long insert_time);

}
