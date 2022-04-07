package ecommerce;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class FraudDetectorService {

	public static void main(String[] args) throws InterruptedException {
		
		var consumer = new KafkaConsumer<String, String>(properties());
		consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));
		
		while(true) {
			var records = consumer.poll(Duration.ofMillis(1000));
			
			if(!records.isEmpty()) {
				System.out.println("Foram encontrados " + records.count() + " registros.");
				
				for(var record : records) {
					System.out.println("************************************************************");
					System.out.println("Processando novas ordens em fraudes");
					System.out.println("key: " + record.key());
					System.out.println("value: " + record.value());
					System.out.println("partition: " + record.partition());
					System.out.println("offset: " + record.offset());
					
					
					Thread.sleep(5000);
				}
			}	
		}
		
	}
	
	public static Properties properties() {
		var properties = new Properties();
		
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.15.5:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());
		return properties;
	}

}
