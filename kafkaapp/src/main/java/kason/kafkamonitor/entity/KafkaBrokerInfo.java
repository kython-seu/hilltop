package kason.kafkamonitor.entity;

import kason.kafkamonitor.inter.JSONParserInter;
import kason.kafkamonitor.utils.DateTimeUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.json.JSONArray;
import scala.util.parsing.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by zhangkai12 on 2017/12/23.
 */
public class KafkaBrokerInfo implements JSONParserInter<KafkaBrokerInfo>{


    private Integer brokerId;
    private String path;

    private Integer jmx_port;
    private String timestamp;
    private String endpoints;
    private String host;
    private Integer version;
    private Integer port;
    private String dateFormat;


    public KafkaBrokerInfo(Integer brokerId, String path, String jsonStr) {
        this.brokerId = brokerId;
        this.path = path;

        parse(jsonStr);
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public Integer getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(Integer brokerId) {
        this.brokerId = brokerId;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }



    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(String endpoints) {
        this.endpoints = endpoints;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getJmx_port() {
        return jmx_port;
    }

    public void setJmx_port(Integer jmx_port) {
        this.jmx_port = jmx_port;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    @Override
    public KafkaBrokerInfo parse(String str) {
        org.json.JSONObject jsonObject = new org.json.JSONObject(str);
        this.jmx_port = jsonObject.getInt("jmx_port");
        this.timestamp = jsonObject.getString("timestamp");
        this.dateFormat = DateTimeUtils.getTimeFormat(timestamp);
        //this.endpoints = jsonObject.getString("endpoints");
        JSONArray jsonArray = jsonObject.getJSONArray("endpoints");
        StringBuilder sb = new StringBuilder("");
        for(int i=0; i<jsonArray.length(); i++){
            sb.append((String)jsonArray.get(0));
        }
        this.endpoints = sb.toString();
        this.host = jsonObject.getString("host");
        this.port = jsonObject.getInt("port");
        this.version = jsonObject.getInt("version");
        return this;
    }

    @Override
    public boolean check() {
        return true;
    }

    @Override
    public String toString() {
        return "KafkaBrokerInfo{" +
                "brokerId=" + brokerId +
                ", path='" + path + '\'' +
                ", jmx_port=" + jmx_port +
                ", timestamp='" + timestamp + '\'' +
                ", endpoints='" + endpoints + '\'' +
                ", host='" + host + '\'' +
                ", version=" + version +
                ", port=" + port +
                ", dateFormat='" + dateFormat + '\'' +
                '}';
    }
}
