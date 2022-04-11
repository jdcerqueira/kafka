package br.com.alura.ecommerce;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorServiceEclipse {

	public static void main(String[] args) {

		var fraudService = new FraudDetectorServiceEclipse();
		try(var service = new KafkaService(fraudService.getClass().getSimpleName(),
				"ECOMMERCE_NEW_ORDER",
				fraudService::parse,
				Order.class,
				Map.of())){
			service.run();
		}
	}
	
	private void parse(ConsumerRecord<String, Order> record) {
		System.out.println("FraudDetectorServiceEclipse:"
				+ "" + record.topic() + "/"
						+ "" + record.partition() + "/"
								+ "" + record.offset() + "/"
										+ "" + record.value() + "/"
												+ "User ID:" + record.value().getUserId());
	}
}
