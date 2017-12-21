package kason.custom.partition.consumer.function2;

import java.util.Arrays;

/**
 * Created by zhangkai12 on 2017/12/21.
 */
public class ConsumerMethod2 {

    public static void main(String[] args) {
        int num_consumer = 3;
        String topic = "demo";
        String group = "group_test";

        final ConsumerRunnable consumerRunnable = new ConsumerRunnable(Arrays.asList(topic), group);

        new Thread(consumerRunnable).start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                /*for (ConsumerLoop consumer : consumers) {
                    consumer.shutdown();
                }
                executorService.shutdown();
                try {
                    executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/
                consumerRunnable.shutdown();
            }
        });

    }
}
