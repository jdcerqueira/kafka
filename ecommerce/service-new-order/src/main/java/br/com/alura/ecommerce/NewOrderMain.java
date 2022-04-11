package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try(var dispatcherOrder = new KafkaDispatcher<Order>();
            var dispatcherEmail = new KafkaDispatcher<Email>()){

            for(int i=0; i<10; i++){
                var userId = UUID.randomUUID().toString();
                var orderId = UUID.randomUUID().toString();
                var value = new BigDecimal(Math.random() * 5000 + 1);
                var order = new Order(userId, orderId, value);

                var subject = "Bem-vindo a nova plataforma de ecommerce.";
                var body = "Corpo do e-mail está aqui.";
                var email = new Email(subject,body);

                dispatcherOrder.send("ECOMMERCE_NEW_ORDER", userId, order);
                dispatcherEmail.send("ECOMMERCE_SEND_EMAIL", userId, email);
            }
        }
    }
}
