package br.com.fuzeto.notificationservice;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class AuditConsumer {

    @KafkaListener(topics = "orders", groupId = "audit-group")
    public void listen(OrderEvent event) {
        System.out.println("AUDIT - order received: " + event);
    }
}
