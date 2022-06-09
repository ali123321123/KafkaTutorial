import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

class Consumer {
    private final Logger mlogger = LoggerFactory.getLogger(Consumer.class.getName());
    private final String mBootstrapServer;
    private final String mGroupId;
    private final String mTopic;


    Consumer(String mBootstrapServer, String mGroupId, String mTopic) {
        this.mBootstrapServer = mBootstrapServer;
        this.mGroupId = mGroupId;
        this.mTopic = mTopic;
    }

    private class ConsumerRunnable implements Runnable {
        private CountDownLatch mLatch;
        private KafkaConsumer<String,String> mConsumer;

        private Properties consumerProps(String bootstrapServer, String groupId) {
            String deserializer = StringDeserializer.class.getName();
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            
            return properties;
        }
        ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch){
            mLatch = latch;
            
            Properties props = consumerProps(bootstrapServer, groupId);
            mConsumer = new KafkaConsumer<>(props);
            mConsumer.subscribe(Collections.singletonList(topic));
        }
        @Override
        public void run(){
            try {
                do {
                    ConsumerRecords<String, String> records = mConsumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        mlogger.info("Key: " + record.key() + " , Value: " + record.value());
                        mlogger.info("Partition: " + record.partition() + ", offset: " + record.offset());
                    }
                }
            while (true);
            }catch (WakeupException e) {
                mlogger.info("Received shutdown signal!");
            }
            finally {
                mConsumer.close();
                mLatch.countDown();
                }
            }
            void shutdown(){
            mConsumer.wakeup();
            }
        }
        
        void run() throws InterruptedException {
        mlogger.info("Creating consumer thread");
        CountDownLatch latch = new CountDownLatch(1);
        
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(mBootstrapServer, mGroupId, mTopic, latch);
        Thread thread = new Thread(consumerRunnable);
        thread.start();
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            mlogger.info("Caught shutdown hook");
            consumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            mlogger.info("Application has exited");
        }));
            latch.await();
        }

    public static void main(String[] args) throws InterruptedException {
        String server = "127.0.0.1:9092";
        String groupId = "some_application";
        String topic = "user_registered";

        new Consumer(server, groupId, topic).run();
    }
        
    }

