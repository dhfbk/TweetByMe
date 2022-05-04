package eu.fbk.dh.twitter;

import eu.fbk.dh.twitter.tables.Option;
import eu.fbk.dh.twitter.tables.OptionRepository;
import eu.fbk.dh.twitter.utils.Defaults;
import org.springframework.stereotype.Service;

import java.util.HashMap;

@Service
public class Options {

    private final OptionRepository optionRepository;
    HashMap<String, String> options = Defaults.getDefaultOptions();

    public Options(OptionRepository optionRepository) {
        this.optionRepository = optionRepository;
        for (Option option : this.optionRepository.findAll()) {
            options.put(option.getId(), option.getValue());
        }
    }

    public String getOption(String key) {
        return options.get(key);
    }
}
