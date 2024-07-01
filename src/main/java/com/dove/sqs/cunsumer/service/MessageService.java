package com.dove.sqs.cunsumer.service;

import com.dove.sqs.cunsumer.AwsSQS;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.model.Message;

@Slf4j
@Service
public class MessageService {
    public void process(Message message, AwsSQS awsSQS) {
        log.info(message.body());

        // 처리한 메시지 제거
        awsSQS.ack(message);
    }
}
