package com.github.skiddyvn.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;

public abstract class RegexFilter<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RegexFilter.class);

  public static final String OVERVIEW_DOC = "";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
  .define(ConfigName.REGEX, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new RegexValidator(), ConfigDef.Importance.HIGH,
          "Regular expression to use for matching.")
  .define(ConfigName.FIELD, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
          "Field string.");

    private interface ConfigName {
      String REGEX = "regex";
      String FIELD = "field";
    }

    private Pattern regex;
    private String field;

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void configure(Map<String, ?> settings) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    regex = Pattern.compile(config.getString(ConfigName.REGEX));
    field = config.getString(ConfigName.FIELD);
  }

  @Override
  public void close() {

  }

  R filter(R record, Struct struct) {
    for (Field f : struct.schema().fields()) {
      if (field == f.name()) {
        if (f.schema().type() == Schema.Type.STRING) {
          String input = struct.getString(f.name());
          if (null != input) {
            Matcher matcher = pattern.matcher(input);
            if (matcher.matches()) {
              return null;
            }
          }
        }
      }
    }
    return record;
  }

  R filter(R record, Map map) {
    for (Object f : map.keySet()) {
      if (field == f) {
        Object value = map.get(f);

        if (value instanceof String) {
          String input = (String) value;
          Matcher matcher = pattern.matcher(input);
          if (matcher.matches()) {
            return null;
          }
        }
      }
    }

    return record;
  }


  R filter(R record, final boolean key) {
    final SchemaAndValue input = key ?
        new SchemaAndValue(record.keySchema(), record.key()) :
        new SchemaAndValue(record.valueSchema(), record.value());
    final R result;
    if (input.schema() != null) {
      if (Schema.Type.STRUCT == input.schema().type()) {
        result = filter(record, (Struct) input.value());
      } else if (Schema.Type.MAP == input.schema().type()) {
        result = filter(record, (Map) input.value());
      } else {
        result = record;
      }
    } else if (input.value() instanceof Map) {
      result = filter(record, (Map) input.value());
    } else {
      result = record;
    }

    return result;
  }

  public static class Key<R extends ConnectRecord<R>> extends RegexFilter<R> {
    @Override
    public R apply(R r) {
      return filter(r, true);
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends RegexFilter<R> {
    @Override
    public R apply(R r) {
      return filter(r, false);
    }
  }
}