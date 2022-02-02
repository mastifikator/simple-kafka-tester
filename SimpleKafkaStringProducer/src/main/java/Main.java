import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {

        if(args.length == 0){
            System.out.println("Please enter arguments");
            System.out.println("Example: simpleKafkaProducer 127.0.0.1:9092 topicName youMessage");
            return;
        }

        String serverAddress = args[0];
        String topicName = args[1];
        String message = args[2];

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<Long, String> producer = new KafkaProducer<>(props);

        producer.send(new ProducerRecord<>(topicName,1L, message));
        System.out.println("Send message: " + message);

        producer.close();
    }
}
