import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {

        if(args.length == 0){
            System.out.println("Please enter arguments");
            System.out.println("Example: simpleKafkaProducer 127.0.0.1:9092 topicName pathToFile");
            return;
        }

        String serverAddress = args[0];
        String topicName = args[1];
        String filePath = args[2];

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<Long, String> producer = new KafkaProducer<>(props);
        FileReader reader = new FileReader(filePath);
        long keyCount = 0;

        try(BufferedReader bufferedReader = new BufferedReader(reader)){
            String line;
            while((line = bufferedReader.readLine()) != null){
                keyCount++;
                producer.send(new ProducerRecord<>(topicName,
                        keyCount, line));
                System.out.println("Send message with key " + keyCount);
            }
        }

        producer.close();
    }
}
