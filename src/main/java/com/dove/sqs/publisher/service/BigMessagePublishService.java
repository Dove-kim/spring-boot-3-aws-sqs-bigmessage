package com.dove.sqs.publisher.service;

import com.amazon.sqs.javamessaging.AmazonSQSExtendedClient;
import com.amazon.sqs.javamessaging.ExtendedClientConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * 대용량 메시지 발송 서비스
 */
@Slf4j
@Service
public class BigMessagePublishService {
    private final SqsClient sqsExtended;
    private final GetQueueUrlResponse queueUrl;

    // DI
    public BigMessagePublishService(
            @Value("${aws.queue.name}") String SQS_QUEUE_NAME,
            @Value("${aws.bucket.name}") String BUCKET_NAME,
            SqsClient sqsClient, S3Client s3Client
    ) {
        ExtendedClientConfiguration extendedClientConfig = new ExtendedClientConfiguration()
                .withPayloadSupportEnabled(s3Client, BUCKET_NAME);
        this.sqsExtended = new AmazonSQSExtendedClient(SqsClient.builder()
                .build(), extendedClientConfig);

        this.queueUrl = sqsClient.getQueueUrl(builder -> builder
                .queueName(SQS_QUEUE_NAME).build());
    }

    /**
     * 메시지 발송
     *
     * @param message 메시지
     */
    public void publish(String message) {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl.queueUrl())
                .messageBody(message)
                .build();
        sqsExtended.sendMessage(sendMessageRequest);
    }
}
