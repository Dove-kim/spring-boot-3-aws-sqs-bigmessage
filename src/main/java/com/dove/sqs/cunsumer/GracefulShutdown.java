package com.dove.sqs.cunsumer;

import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

@Component
public class GracefulShutdown implements ApplicationListener<ContextClosedEvent> {

    private final ThreadPoolTaskExecutor taskExecutor;

    public GracefulShutdown(ThreadPoolTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        this.taskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        this.taskExecutor.setAwaitTerminationSeconds(300);
    }
}
