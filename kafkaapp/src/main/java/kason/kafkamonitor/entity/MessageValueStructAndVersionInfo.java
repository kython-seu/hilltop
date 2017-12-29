package kason.kafkamonitor.entity;

import org.apache.kafka.common.protocol.types.Struct;

/**
 * Created by zhangkai12 on 2017/12/29.
 */
public class MessageValueStructAndVersionInfo {

    private Struct value;
    private Short version;

    public Struct getValue() {
        return value;
    }

    public void setValue(Struct value) {
        this.value = value;
    }

    public Short getVersion() {
        return version;
    }

    public void setVersion(Short version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "MessageValueStructAndVersionInfo{" +
                "value=" + value +
                ", version=" + version +
                '}';
    }
}
