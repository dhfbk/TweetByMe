package eu.fbk.dh.twitter.tables;

import lombok.Data;

import javax.persistence.*;

@Entity
@Data
@Table
public class HistoryTag {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
    private Long session_id;
    private String tag;
    private String lang;
}
