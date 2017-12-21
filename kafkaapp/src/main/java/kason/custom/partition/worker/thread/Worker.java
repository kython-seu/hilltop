package kason.custom.partition.worker.thread;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by zhangkai12 on 2017/12/20.
 */
public class Worker implements Runnable {

    private ConsumerRecord<String, String> record;

    public Worker(ConsumerRecord<String, String> record) {
        this.record = record;
    }

    @Override
    public void run() {

        /*try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/

        System.out.println(Thread.currentThread().getName() + "====" + record.value() + "===" + record.offset());
    }
}
