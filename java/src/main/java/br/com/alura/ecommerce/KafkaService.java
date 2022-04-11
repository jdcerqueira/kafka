package br.com.alura.ecommerce;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

class KafkaService<T> implements Closeable {
	

	private final KafkaConsumer<String, T> consumer;
	private final IConsumerFunction parse;

	KafkaService (String groupId, String topic, IConsumerFunction parse, Class<T> type, Map<String, String> properties) {
		this(groupId, parse, type, properties);
		this.consumer.subscribe(Collections.singletonList(topic));
	}
	
	KafkaService (String groupId, IConsumerFunction parse, Class<T> type, Map<String, String> properties) {
		this.consumer = new KafkaConsumer<String, T>(getProperties(groupId,type,properties));
		this.parse = parse;
	}
	
	void run() {
		while(true) {
			var records = this.consumer.poll(Duration.ofMillis(100));
			if(!records.isEmpty())
				for(var record:records)
					this.parse.consume(record);
		}
	}

	private Properties getProperties(String groupId, Class<T> type, Map<String, String> overrideProperties) {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.15.5:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
		
		return properties;
	}

	@Override
	public void close(){
		this.consumer.close();
	}

}
