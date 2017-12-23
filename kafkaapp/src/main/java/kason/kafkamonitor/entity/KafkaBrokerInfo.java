package kason.kafkamonitor.entity;

import kason.kafkamonitor.inter.JSONParserInter;

/**
 * Created by zhangkai12 on 2017/12/23.
 */
public class KafkaBrokerInfo implements JSONParserInter<KafkaBrokerInfo>{


    private Integer brokerId;
    private String path;

    private String jmx_port;
    private String timestamp;
    private String endpoints;
    private String host;
    private String version;
    private String port;

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

    public String getJmx_port() {
        return jmx_port;
    }

    public void setJmx_port(String jmx_port) {
        this.jmx_port = jmx_port;
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

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    @Override
    public KafkaBrokerInfo parse(String str) {
        return null;
    }

    @Override
    public boolean check() {
        return false;
    }
}
