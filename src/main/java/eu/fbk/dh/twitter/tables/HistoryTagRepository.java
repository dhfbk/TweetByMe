package eu.fbk.dh.twitter.tables;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface HistoryTagRepository extends CrudRepository<HistoryTag, Long> {

    @Query("FROM HistoryTag WHERE session_id >= :start_time AND session_id <= :end_time")
    List<HistoryTag> getTagsWithInterval(Long start_time, Long end_time);

}
