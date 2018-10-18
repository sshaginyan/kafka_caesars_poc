package com.example.poc1v1.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class RestApiController {

    @PostMapping("/send-message")
    public ResponseEntity<Void> sendMessage(){
        System.out.println("Testing123");
        return null;
    }

}
