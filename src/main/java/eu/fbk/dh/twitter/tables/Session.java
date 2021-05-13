package eu.fbk.dh.twitter.tables;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Data
@Table
public class Session {

    @Id
    private Long session_id;
    private Long start_time, end_time;
    private Boolean done = false;

}
