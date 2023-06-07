package Db2Db.mongoListenerToRabbitmq.rabbitmq;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class RabbitService {
    private final RabbitTemplate rabbitTemplate;

    @Value("${spring.rabbitmq.exchange}")
    String exchange;

    @Value("${spring.rabbitmq.routingKey}")
    String routingKey;

    public RabbitService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void sendMessage(Object message){
        System.out.println("Sent message to rabbitmq");
        rabbitTemplate.convertAndSend(exchange, routingKey, message);
    }
}
