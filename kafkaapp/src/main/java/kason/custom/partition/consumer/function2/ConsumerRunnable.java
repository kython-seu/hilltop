package kason.custom.partition.consumer.function2;

import kason.custom.partition.Worker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhangkai12 on 2017/12/20.
 */
public class ConsumerRunnable implements Runnable {

    private KafkaConsumer<String, String> kafkaConsumer;
    private List<String> topics;
    private String group;
    private int executorsNum = 10;
    private ThreadPoolExecutor threadPoolExecutor;
    private BlockingQueue<Runnable> blockingQueue=new ArrayBlockingQueue<>(10000);
    public ConsumerRunnable( List<String> topics, String group) {

        this.topics = topics;
        this.group = group;
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("group.id", group);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.kafkaConsumer = new KafkaConsumer<>(props);
        threadPoolExecutor = new ThreadPoolExecutor(executorsNum/2,executorsNum,1, TimeUnit.MINUTES,blockingQueue);
    }

    @Override
    public void run() {
        try {
            kafkaConsumer.subscribe(topics);
            while (true) {
                ConsumerRecords<String, String> poll = kafkaConsumer.poll(Integer.MAX_VALUE);
                for (ConsumerRecord<String, String> record : poll) {
                    System.out.println("submit Worker Thread");
                    //new Thread(new Worker(record)).setName("Thread-").start();
                    //new Thread(new Worker(record)).start();
                    threadPoolExecutor.submit(new Worker(record));
                }
            }
        }catch (WakeupException e){

        }finally {
            System.out.println("release resource");
            kafkaConsumer.close();
            threadPoolExecutor.shutdown();
        }
    }

    public void shutdown(){
        kafkaConsumer.wakeup();
    }
}
