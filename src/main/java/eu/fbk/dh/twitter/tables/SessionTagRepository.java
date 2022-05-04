package eu.fbk.dh.twitter.tables;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import javax.transaction.Transactional;
import java.util.List;

public interface SessionTagRepository extends CrudRepository<SessionTag, Long> {
    @Transactional
    @Modifying
    @Query("UPDATE SessionTag SET next_token = :next_token WHERE tag = :tag AND session_id = :session_id")
    void updateNextToken(String next_token, String tag, Long session_id);

    @Query("FROM SessionTag WHERE session_id = :session_id")
    List<SessionTag> getSessionTagsPerSession(Long session_id);

    @Query("FROM SessionTag WHERE done = 0 ORDER BY session_id, tag")
    List<SessionTag> getUnfinishedSessionTags();

    @Transactional
    @Modifying
    @Query("UPDATE SessionTag SET done = 1, next_token = '' WHERE tag = :tag AND session_id = :session_id")
    void updateDone(String tag, Long session_id);
}
