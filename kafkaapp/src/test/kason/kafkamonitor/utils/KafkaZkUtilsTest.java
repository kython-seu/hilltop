package kason.kafkamonitor.utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by zhangkai12 on 2017/12/26.
 */
public class KafkaZkUtilsTest {
    @Test
    public void getAllTopics() throws Exception {
        KafkaZkUtils.getAllTopics("BAYONET_VEHICLEPASS");
    }

}