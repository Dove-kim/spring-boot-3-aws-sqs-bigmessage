package com.dove.sqs.cunsumer;

import com.amazon.sqs.javamessaging.AmazonSQSExtendedClient;
import com.amazon.sqs.javamessaging.ExtendedClientConfiguration;
import com.dove.sqs.cunsumer.service.MessageService;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.concurrent.CompletableFuture;


/**
 * SQS 메시지 Pooling 객체
 * SqsClient, ThreadPoolTaskExecutor를 이용해 SQS에서 메시지를 받는다.
 */
@Slf4j
@Component
public class SqsExtendedClientPolling implements AwsSQS {
    private static final int THREAD_SLEEP_TIME = 1000;
    private final ThreadPoolTaskExecutor consumerThreadPoolTaskExecutor;

    private final GetQueueUrlResponse queueUrl;
    private SqsClient sqsExtended;

    private final MessageService messageService;

    // DI
    public SqsExtendedClientPolling(
            @Value("${aws.queue.name}") String SQS_QUEUE_NAME,
            @Value("${aws.bucket.name}") String BUCKET_NAME,
            ThreadPoolTaskExecutor consumerThreadPoolTaskExecutor,
            SqsClient sqsClient, S3Client s3Client,
            MessageService messageService
    ) {
        // SQS client
        ExtendedClientConfiguration extendedClientConfig = new ExtendedClientConfiguration().withPayloadSupportEnabled(s3Client, BUCKET_NAME);
        this.sqsExtended = new AmazonSQSExtendedClient(SqsClient.builder().build(), extendedClientConfig);

        // SQS queue url
        this.queueUrl = sqsClient.getQueueUrl(builder -> builder
                .queueName(SQS_QUEUE_NAME).build());

        // bean setting
        this.messageService = messageService;
        this.consumerThreadPoolTaskExecutor = consumerThreadPoolTaskExecutor;
    }

    /**
     * SQS 풀링을 하기위한 기본적인 세팅을 담당한다.
     */
    @PostConstruct
    public void initiateMessageProcessing() {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl.queueUrl())
                .maxNumberOfMessages(10)
                .waitTimeSeconds(0)
                .build();

        startPolling(receiveMessageRequest);
    }

    /**
     * SQS 풀링을 시작한다.
     *
     * @param receiveMessageRequest {@link ReceiveMessageRequest}
     */
    private void startPolling(ReceiveMessageRequest receiveMessageRequest) {
        CompletableFuture.runAsync(() -> {
            while (true) {
                try {
                    log.debug("Polling queue");
                    ReceiveMessageResponse receiveMessageResponse = sqsExtended.receiveMessage(receiveMessageRequest);
                    if (receiveMessageResponse.hasMessages()) {
                        log.debug("Message received: {}", receiveMessageResponse.messages());
                        receiveMessageResponse.messages().forEach(this::processMessageIfThreadAvailable);
                    }
                } catch (Exception e) {
                    log.error(e.toString(), e);
                }
            }
        });
    }

    /**
     * message를 처리한다.
     * 처리하는 message가 consumerThreadPoolTaskExecutor.getMaxPoolSize() 기준 같거나 많을 만들경우
     * {@value THREAD_SLEEP_TIME} millisecond 만큼 대기한 뒤 메시지를 처리한다.
     *
     * @param message {@link Message}
     */
    private void processMessageIfThreadAvailable(Message message) {
        if (consumerThreadPoolTaskExecutor.getActiveCount() < consumerThreadPoolTaskExecutor.getMaxPoolSize()) {
            consumerThreadPoolTaskExecutor.execute(() -> messageService.process(message, this));
        } else {
            try {
                Thread.sleep(THREAD_SLEEP_TIME);
            } catch (InterruptedException e) {
                log.error(e.toString(), e);
            }
        }
    }

    /**
     * 메지지를 삭제한다.
     *
     * @param message {@link Message}
     */
    @Override
    public void ack(Message message) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl.queueUrl())
                .receiptHandle(message.receiptHandle())
                .build();
        sqsExtended.deleteMessage(deleteMessageRequest);
    }

}
