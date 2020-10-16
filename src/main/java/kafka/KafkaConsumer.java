package kafka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ramindu on 1/9/17.
 */
public class KafkaConsumer implements Runnable  {
    private org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> consumer;
    private List<TopicPartition> partitionsList = new ArrayList<>();
    private Log log = LogFactory.getLog(KafkaConsumer.class);
    private String group = "spconsumergroup";
    private String bootstrapServer = "localhost:9092";
    private String topic = "sandpglobalConsumer";
    private int partition = -1;
    private long timeDuration = 60*1000;

    public KafkaConsumer(String bootstrapServer, String topic, int partition, String group, long timeDuration) {
        if (bootstrapServer != null) {
            this.bootstrapServer = bootstrapServer;
        }
        if (topic != null) {
            this.topic = topic;
        }
        if (partition != -1) {
            this.partition = partition;
        }
        if (group != null) {
            this.group = group;
        }
        if (timeDuration != -1) {
            this.timeDuration = timeDuration;
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", this.bootstrapServer);
        props.put("group.id", this.group);
        props.put("session.timeout.ms", "30000");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        if (partition != -1) {
            TopicPartition topicPartition = new TopicPartition("kafka_partitioned_topic", this.partition);
            partitionsList.add(topicPartition);
            consumer.assign(partitionsList);
        } else {
            consumer.subscribe(Arrays.asList(this.topic));
            System.out.println("subscribing to topic: " + this.topic);
        }
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        long endTime = System.currentTimeMillis() + timeDuration;
        AtomicLong TPSRecords = new AtomicLong(0);
        AtomicLong totalRecordCount = new AtomicLong(0);
        System.out.println("Starting time: " + (new Date()));
        System.out.println("running !!!!!!!!!! " + endTime + " > " + System.currentTimeMillis());
        long fromSourceToThrouputClient = 0;
        long fromSinkToThroughputClient = 0;
        long fromSourcetoSink = 0;
        long fromThroughputClientToSource = 0;
        long fullLatency = 0;
        while (endTime > System.currentTimeMillis()) {
            ConsumerRecords<byte[], byte[]> records = null;
            try {
                // takes time and if this value is small, there will be an CommitFailedException while
                // trying to retrieve data
                records = consumer.poll(100);
            } catch (CommitFailedException ex) {
                System.out.println("Consumer poll() failed." + ex.getMessage());
            }
            long currentTimestamp = System.currentTimeMillis();
            if (null != records) {
                long recCount = records.count();
                TPSRecords.addAndGet(recCount);
                totalRecordCount.addAndGet(recCount);
                for (ConsumerRecord record : records) {
                    JSONObject metaJsonObject = new JSONObject(record.value().toString()).getJSONObject("metadata");
                    fromThroughputClientToSource +=
                            Long.parseLong(metaJsonObject.getString("consumer_out_timestamp")) -
                                    Long.parseLong(metaJsonObject.getString("consumer_in_timestamp"));
                    fromSourceToThrouputClient +=
                            currentTimestamp - Long.parseLong(metaJsonObject.getString("consumer_out_timestamp"));
                    fromSinkToThroughputClient +=
                            currentTimestamp - record.timestamp();
                    fromSourcetoSink +=
                            record.timestamp() - Long.parseLong(metaJsonObject.getString("consumer_out_timestamp"));
                    fullLatency +=
                            currentTimestamp - Long.parseLong(metaJsonObject.getString("consumer_in_timestamp"));
                }

                if (startTime + 1000 < currentTimestamp) {
                    if (TPSRecords.get() != 0) {
                        System.out.println(
                                "Receiving TPS: " + TPSRecords.get() + " totalReceivedCount: " + totalRecordCount.get() +
                                        " fromThroughputClientToSource: " + fromThroughputClientToSource/TPSRecords.get() +
                                        " fromSourceToThrouputClient: " + fromSourceToThrouputClient/TPSRecords.get() +
                                        " fromSinkToThroughputClient: " + fromSinkToThroughputClient/TPSRecords.get() +
                                        " fromSourcetoSink: " + fromSourcetoSink/TPSRecords.get() +
                                        " fullLatency: " + fullLatency/TPSRecords.get());
                    } else {
                        System.out.println("No event recieved. totalReceivedCount: " + totalRecordCount);
                    }
                    TPSRecords = new AtomicLong(0);
                    startTime = currentTimestamp;
                    fromSourceToThrouputClient = 0;
                    fromSinkToThroughputClient = 0;
                    fromThroughputClientToSource = 0;
                    fromSourcetoSink = 0;
                    fullLatency = 0;
                }
            } else {
                if (startTime + 1000 < currentTimestamp) {
                    System.out.println("No event recieved. totalReceivedCount: " + totalRecordCount);
                    TPSRecords = new AtomicLong(0);
                    startTime = currentTimestamp;
                    fromSourceToThrouputClient = 0;
                    fromSinkToThroughputClient = 0;
                    fromThroughputClientToSource = 0;
                    fromSourcetoSink = 0;
                    fullLatency = 0;
                }
            }
            try { //To avoid thread spin
                Thread.sleep(1);
            } catch (InterruptedException e) {
                System.out.println("InterruptedException: " + e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("Ending time: " + (new Date()));
    }
}
