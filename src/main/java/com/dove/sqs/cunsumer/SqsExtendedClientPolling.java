package com.dove.sqs.cunsumer;

import com.amazon.sqs.javamessaging.AmazonSQSExtendedClient;
import com.amazon.sqs.javamessaging.ExtendedClientConfiguration;
import com.dove.sqs.cunsumer.service.MessageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.task.ThreadPoolTaskExecutorBuilder;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * SQS 메시지 Pooling 객체
 * SqsClient, ThreadPoolTaskExecutor를 이용해 SQS에서 메시지를 받는다.
 */
@Slf4j
@Component
public class SqsExtendedClientPolling implements AwsSQS, SmartLifecycle {
    /**
     * 풀링 작동 여부
     */
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    /**
     * 풀링 대기 시간
     * 설정한 시간만큼 1회 메시지를 가져오고 대기한다.
     */
    private static final int THREAD_SLEEP_TIME = 1000;
    /**
     * 한번 메시지를 풀링할때 가져올 최대 메시지 수
     */
    private static final int MAX_MESSAGE_COUNT_PER_REQUEST = 10;

    /**
     * 메시지 처리를 위한 쓰레드풀
     */
    private final ThreadPoolTaskExecutor consumerThreadPoolTaskExecutor;

    /**
     * 풀링 작업을 진행하는 CompletableFuture
     */
    private Future<?> poller;

    /**
     * 풀링할 AWS SQS 큐 정보
     */
    private final GetQueueUrlResponse queueUrl;
    /**
     * AWS SQS client
     */
    private final SqsClient sqsExtended;

    /**
     * SQS 메시지 처리 서비스
     */
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
     * SQS 풀링을 시작한다.
     */
    private void startPolling() {
        poller = CompletableFuture.runAsync(() -> {
            while (isRunning.get()) {
                try {
                    Thread.sleep(THREAD_SLEEP_TIME);

                    // Thread가 모두 사용중이면 continue;
                    if (consumerThreadPoolTaskExecutor.getActiveCount() >= consumerThreadPoolTaskExecutor.getMaxPoolSize()) {
                        continue;
                    }

                    // 사용가능한 쓰레드 수를 가져온다.
                    int availableThreadCount = getAvailableThreadCount();

                    // 메시지를 가져온다.
                    ReceiveMessageResponse receiveMessageResponse = sqsExtended
                            .receiveMessage(ReceiveMessageRequest.builder()
                                    .queueUrl(queueUrl.queueUrl())
                                    .maxNumberOfMessages(availableThreadCount)
                                    .waitTimeSeconds(0)
                                    .build());

                    // 메시지가 없을 경우 continue
                    if (!receiveMessageResponse.hasMessages()) {
                        continue;
                    }
                    log.debug("Message received: {}", receiveMessageResponse.messages());

                    // 메시지를 처리한다.
                    for (Message message : receiveMessageResponse.messages()) {
                        consumerThreadPoolTaskExecutor.execute(() -> messageService.process(message, this));
                    }

                } catch (Exception e) {
                    log.error(e.toString(), e);
                }
            }
        }, Executors.newFixedThreadPool(1, new ThreadPoolTaskExecutorBuilder()
                .threadNamePrefix("sqs-poling-")
                .build()));


    }

    /**
     * 사용 가능한 쓰레드 수를 리턴한다.
     * 단, {@link #MAX_MESSAGE_COUNT_PER_REQUEST}보다 사용가능한 쓰레드가 많을 경우
     * {@link #MAX_MESSAGE_COUNT_PER_REQUEST}를 리턴한다.
     *
     * @return 사용가능한 쓰레드 수
     */
    private int getAvailableThreadCount() { // Extracted method
        int availableThreadCount = consumerThreadPoolTaskExecutor.getMaxPoolSize() -
                consumerThreadPoolTaskExecutor.getActiveCount();
        return Math.min(availableThreadCount, MAX_MESSAGE_COUNT_PER_REQUEST);
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

    @Override
    public void start() {
        isRunning.set(true);

        startPolling();
    }

    @Override
    public void stop() {
        isRunning.set(false);
        if (poller != null && !poller.isDone()) {
            try {
                // CompletableFuture 작업이 끝날 때까지 기다린다.
                poller.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error(e.toString(), e);
            }
        }
    }

    @Override
    public boolean isRunning() {
        return isRunning.get();
    }

    @Override
    public void stop(Runnable callback) {
        this.stop();
        callback.run();
    }

    @Override
    public int getPhase() {
        return 0;
    }

}
