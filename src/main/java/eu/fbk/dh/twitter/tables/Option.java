package eu.fbk.dh.twitter.tables;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Data
@Table
public class Option {

    @Id
    private String id;
    private String value;
}
