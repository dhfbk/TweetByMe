package eu.fbk.dh.twitter.tables;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import javax.transaction.Transactional;
import java.util.List;

public interface SessionRepository extends CrudRepository<Session, Long> {
    @Query("FROM Session ORDER BY session_id DESC")
    List<Session> getSessionsSortedByTimeDesc();

    @Query("FROM Session WHERE done = 0 AND session_id != :session_id ORDER BY session_id")
    List<Session> getUnfinishedSessions(Long session_id);

    @Transactional
    @Modifying
    @Query("UPDATE Session SET done = 1 WHERE done = 0 AND session_id != :session_id")
    void setSessionAsDone(Long session_id);
}
