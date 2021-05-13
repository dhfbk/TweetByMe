package eu.fbk.dh.twitter.tables;

import lombok.Data;

import javax.persistence.*;

@Entity
@Data
@Table(indexes = {
        @Index(name = "insert_time_index", columnList = "insert_time"),
        @Index(name = "start_time_index", columnList = "start_time"),
        @Index(name = "expired_time_index", columnList = "expired_time")
})
public class Tag {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private String tag;
    private Long insert_time, start_time, expired_time;
    private Boolean done = false;
    private String next_token = "";

}
