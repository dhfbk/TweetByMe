package eu.fbk.dh.twitter.tables;

import org.springframework.data.repository.CrudRepository;

public interface OptionRepository extends CrudRepository<Option, String> {

    default void saveOption(String id, String value) {
        Option option = new Option();
        option.setId(id);
        option.setValue(value);
        save(option);
    }
}
