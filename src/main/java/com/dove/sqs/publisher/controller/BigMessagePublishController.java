package com.dove.sqs.publisher.controller;

import com.dove.sqs.publisher.service.BigMessagePublishService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
public class BigMessagePublishController {
    private final BigMessagePublishService bigMessagePublishService;

    @PostMapping("/push")
    public ResponseEntity<Void> push(@RequestBody String message) {
        bigMessagePublishService.publish(message);

        return ResponseEntity.ok().build();
    }
}
