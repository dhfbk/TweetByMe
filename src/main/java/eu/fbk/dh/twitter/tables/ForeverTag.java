package eu.fbk.dh.twitter.tables;

import lombok.Data;

import javax.persistence.*;

@Entity
@Data
@Table
public class ForeverTag {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
    private Long insert_time;
    private Long start_time;
    private String tag;
    private String lang;
    private Boolean active = true;
    private Boolean done = false;
    private String next_token = "";
}
