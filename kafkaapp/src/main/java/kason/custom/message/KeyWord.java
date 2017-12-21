package kason.custom.message;


import org.apache.kafka.common.serialization.Deserializer;

import java.util.Date;
import java.util.Map;

/**
 * Created by zhangkai12 on 2017/7/31.
 */
public class KeyWord implements org.apache.kafka.common.serialization.Serializer<KeyWord>,Deserializer<KeyWord> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, KeyWord data) {
        System.out.println("data " + data);
        return data.toString().getBytes();
    }

    @Override
    public void close() {

    }

    @Override
    public KeyWord deserialize(String topic, byte[] data) {

        return new KeyWord();
    }

    private long id;
    private String user;
    private String keyWord;
    private Date date;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getKeyWord() {
        return keyWord;
    }

    public void setKeyWord(String keyWord) {
        this.keyWord = keyWord;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "KeyWord{" +
                "id=" + id +
                ", user='" + user + '\'' +
                ", keyWord='" + keyWord + '\'' +
                ", date=" + date +
                '}';
    }
}
