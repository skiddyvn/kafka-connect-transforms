package com.github.skiddyvn.kafka.connect.transforms;

import com.github.skiddyvn.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public abstract class DataTypeFilter<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "Field string. Format: field1:type1,field2:type2,... Example: field1:java.lang.String,field2:java.lang.Number");
    private static final Logger LOGGER = LoggerFactory.getLogger(DataTypeFilter.class);
    private String field;

    public static boolean isInstanceOf(Object obj, String type) {
        try {
            Class<?> clazz = Class.forName(type);
            return clazz.isInstance(obj);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        field = config.getString(ConfigName.FIELD);
    }

    @Override
    public void close() {

    }

    R filter(R record, Struct struct, Map<String, String> mapPair) {
        for (Field f : struct.schema().fields()) {
            String fieldDataType = mapPair.get(f.name());
            if (fieldDataType != null) {
                Object value = struct.get(f);
                if (fieldDataType != null && !isInstanceOf(value, fieldDataType)) {
                    return null;
                }
            }
        }
        return record;
    }

    R filter(R record, Map map, Map<String, String> mapPair) {
        for (Object f : map.keySet()) {
            String fieldDataType = mapPair.get(f);
            if (fieldDataType != null) {
                Object value = map.get(f);
                if (fieldDataType != null && !isInstanceOf(f, fieldDataType)) {
                    return null;
                }
            }
        }
        return record;
    }

    R filter(R record, final boolean key) {
        if (field.indexOf(':') == -1) {
            return record;
        }
        ArrayList<String> fieldPairs = new ArrayList<>(Arrays.asList(field.split(",")));
        Map<String, String> mapFieldPair = new HashMap<>();
        for (String pair : fieldPairs) {
            String[] kv = pair.split(":");
            if (kv.length != 2) {
                continue;
            }
            mapFieldPair.put(kv[0], kv[1]);
        }
        final SchemaAndValue input = key ?
                new SchemaAndValue(record.keySchema(), record.key()) :
                new SchemaAndValue(record.valueSchema(), record.value());
        final R result;
        if (input.schema() != null) {
            if (Schema.Type.STRUCT == input.schema().type()) {
                result = filter(record, (Struct) input.value(), mapFieldPair);
            } else if (Schema.Type.MAP == input.schema().type()) {
                result = filter(record, (Map) input.value(), mapFieldPair);
            } else {
                result = record;
            }
        } else if (input.value() instanceof Map) {
            result = filter(record, (Map) input.value(), mapFieldPair);
        } else {
            result = record;
        }

        return result;
    }
    private interface ConfigName {
        String FIELD = "field";
    }

    public static class Key<R extends ConnectRecord<R>> extends DataTypeFilter<R> {
        @Override
        public R apply(R r) {
            return filter(r, true);
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends DataTypeFilter<R> {
        @Override
        public R apply(R r) {
            return filter(r, false);
        }
    }
}