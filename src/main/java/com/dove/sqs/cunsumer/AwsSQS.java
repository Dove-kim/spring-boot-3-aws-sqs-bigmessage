package com.dove.sqs.cunsumer;

import software.amazon.awssdk.services.sqs.model.Message;

public interface AwsSQS {
    void ack(Message message);
}
