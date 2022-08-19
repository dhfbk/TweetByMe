package eu.fbk.dh.twitter.tables;


import lombok.Data;

import javax.persistence.*;

@Entity
@Data
@Table(
        indexes = {@Index(name = "st_index", columnList = "tag, session_id, lang", unique = true)}
)
public class SessionTag {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private String tag;
    private String lang;
    private Long session_id, start_time, end_time;
    private Boolean done = false;
    private String next_token = "";

}
