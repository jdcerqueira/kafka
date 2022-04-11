package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class EmailService {

    public static void main(String[] args) {
        var emailService = new EmailService();
        try(var service = new KafkaService(emailService.getClass().getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                Email.class,
                Map.of())){
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Email> record) {
        System.out.println("EmailService: "+ record.topic()+ "/" + record.key() + "/" + record.value() + "/" + record.partition() + "/" + record.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
