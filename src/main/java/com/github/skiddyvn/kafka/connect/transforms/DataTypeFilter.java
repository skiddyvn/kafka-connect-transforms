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

public abstract class DataTypeFilter<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataTypeFilter.class);

  public static final String OVERVIEW_DOC = "";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
  .define(ConfigName.FIELD, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
          "Field string.");

    private interface ConfigName {
      String FIELD = "field";
    }

    private String field;

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void configure(Map<String, ?> settings) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    field = config.getString(ConfigName.FIELD);
  }

  @Override
  public void close() {

  }

  @Override
  public R apply(R record) {
    if (!field.includes(":")) {
        return record;
    }
    final SchemaAndValue input = key ?
    new SchemaAndValue(record.keySchema(), record.key()) :
    new SchemaAndValue(record.valueSchema(), record.value());
    final R result;
    if (input.schema() != null) {
        return record;
    } else if (input.value() instanceof Map) {
        ArrayList<String> pairs = new ArrayList<>(Arrays.asList(field.split(",")));
        Map<String, Object> mapValue = (Map<String, Object>) input.value();
        Map<String, String> mapPair = new HashMap<>();
        for (String pair : pairs) {
            String[] kv = pair.split(":");
            mapPair.put(kv[0], kv[1]);
        }
        for (Object f : mapValue.keySet()) {
            String fieldDataType = mapPair.get(f);
            if (fieldDataType != null) {
                if (fieldDataType.equals("string") && !mapValue.get(f) instanceof String) {
                    return null;
                } else if (fieldDataType.equals("int") && !mapValue.get(f) instanceof Integer) {
                    return null;
                } else if (fieldDataType.equals("long") && !mapValue.get(f) instanceof Long) {
                    return null;
                } else if (fieldDataType.equals("float") && !mapValue.get(f) instanceof Float) {
                    return null;
                } else if (fieldDataType.equals("double") && !mapValue.get(f) instanceof Double) {
                    return null;
                } else if (fieldDataType.equals("boolean") && !mapValue.get(f) instanceof Boolean) {
                    return null;
                }
            }
        }
    } else {
        return record;
    }
    return result;
  }
}