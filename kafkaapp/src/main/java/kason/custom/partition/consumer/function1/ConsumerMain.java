package kason.custom.partition.consumer.function1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhangkai12 on 2017/12/20.
 */
public class ConsumerMain {

    public static void main(String[] args) {

        int num_consumer = 3;
        String topic = "test";
        String group = "group_test";

        List<String> topics = Arrays.asList(topic);
        final List<ConsumerLoop> consumers = new ArrayList<>();
        final ExecutorService executorService = Executors.newFixedThreadPool(num_consumer);
        for(int i = 0; i < num_consumer; i++){
            ConsumerLoop consumerLoop = new ConsumerLoop(i,topics, group);
            consumers.add(consumerLoop);
            executorService.submit(consumerLoop);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (ConsumerLoop consumer : consumers) {
                    consumer.shutdown();
                }
                executorService.shutdown();
                try {
                    executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
