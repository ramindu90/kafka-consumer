package kafka;

public class ConsumerRunner {
    public static void main(String[] args) {
        long timeDuration;
        if (args.length >= 1) {
            timeDuration = Long.parseLong(args[0])*60*1000;
        } else {
            timeDuration = 1*60*1000;
        }
        String group;
        if (args.length >= 2) {
            group = args[1];
        } else {
            group = "spconsumergroup";
        }
        String bootstrapServer;
        if (args.length >= 3) {
            bootstrapServer = args[2];
        } else {
            bootstrapServer = "localhost:9092";
        }
        String topic;
        if (args.length >= 4) {
            topic = args[3];
        } else {
            topic = "sandpglobalConsumer";
        }
        int partition;
        if (args.length >= 5) {
            partition = Integer.parseInt(args[4]);
        } else {
            partition = -1;
        }
        KafkaConsumer kafkaConsumer1 = new KafkaConsumer(bootstrapServer, topic, partition, group, timeDuration);
        Thread thread = new Thread(kafkaConsumer1);
        thread.start();
    }
}
