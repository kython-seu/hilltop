package kason.custom.avro_kafka;

import kason.custom.partition.consumer.function1.ConsumerLoop;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhangkai12 on 2017/12/22.
 */
public class Main {
    public static void main(String[] args) {
        AvroKafkaConsumer avroKafkaConsumer = new AvroKafkaConsumer(Arrays.asList("avro"), "avro_test");
        avroKafkaConsumer.testConsumer();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                avroKafkaConsumer.shutdown();
            }
        });
    }
}
