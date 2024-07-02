package com.dove.sqs.cunsumer.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * Gradceful Shutdown 등록
 */
@Component
@RequiredArgsConstructor
public class GracefulShutdown implements ApplicationListener<ContextClosedEvent> {
    private final ThreadPoolTaskExecutor consumerThreadPoolTaskExecutor;

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        this.consumerThreadPoolTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        this.consumerThreadPoolTaskExecutor.setAwaitTerminationSeconds(300);
    }
}
