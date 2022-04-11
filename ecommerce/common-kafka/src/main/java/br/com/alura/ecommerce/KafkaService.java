package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    KafkaService(String groupId, String topic, ConsumerFunction parse, Class<T> type, Map<String,String> properties) {
        this(groupId,parse,type, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    KafkaService(String groupId, Pattern topic, ConsumerFunction parse, Class<T> type, Map<String,String> properties) {
        this(groupId,parse, type, properties);
        consumer.subscribe(topic);
    }

    private KafkaService(String groupId, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this.consumer = new KafkaConsumer<>(getProperties(type, groupId, properties));
        this.parse = parse;
    }

    void run(){
        while(true){
            var records = this.consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty())
                for(var record : records)
                    this.parse.consume(record);
        }
    }

    private Properties getProperties(Class<T> type, String groupId, Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.15.5:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"1");
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());

        properties.putAll(overrideProperties);

        return properties;
    }

    @Override
    public void close(){
        this.consumer.close();
    }
}
