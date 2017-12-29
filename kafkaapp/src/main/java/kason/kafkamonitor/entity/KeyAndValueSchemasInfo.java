package kason.kafkamonitor.entity;

import org.apache.kafka.common.protocol.types.Schema;

/**
 * Created by zhangkai12 on 2017/12/29.
 */
public class KeyAndValueSchemasInfo {
    private Schema keySchema;
    private Schema valueSchema;

    public Schema getKeySchema() {
        return keySchema;
    }

    public void setKeySchema(Schema keySchema) {
        this.keySchema = keySchema;
    }

    public Schema getValueSchema() {
        return valueSchema;
    }

    public void setValueSchema(Schema valueSchema) {
        this.valueSchema = valueSchema;
    }

    @Override
    public String toString() {
        return "KeyAndValueSchemasInfo{" +
                "keySchema=" + keySchema +
                ", valueSchema=" + valueSchema +
                '}';
    }
}
