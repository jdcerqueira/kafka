package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class FraudDetectorService {

    public static void main(String[] args) {
        var fraudService = new FraudDetectorService();
        try(var service = new KafkaService<>(fraudService.getClass().getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                Order.class,
                Map.of())){
            service.run();
        }

    }

    private void parse(ConsumerRecord<String, Order> record){
        System.out.println("FraudDetectorService: "+ record.topic()+ "/" + record.key() + "/" + record.value() + "/" + record.partition() + "/" + record.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
