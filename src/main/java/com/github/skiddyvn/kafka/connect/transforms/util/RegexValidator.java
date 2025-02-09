package com.github.skiddyvn.kafka.connect.transforms.util;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.regex.Pattern;

public class RegexValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(String name, Object value) {
        try {
            Pattern.compile((String) value);
        } catch (Exception e) {
            throw new ConfigException(name, value, "Invalid regex: " + e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "valid regex";
    }

}