package com.github.skiddyvn.kafka.connect.transforms.util;


import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * A barebones concrete implementation of {@link AbstractConfig}.
 */
public class SimpleConfig extends AbstractConfig {

    public SimpleConfig(ConfigDef configDef, Map<?, ?> originals) {
        super(configDef, originals, false);
    }

}
