package com.example.poc1v1.controller;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.poc1v1.model.Data;

@RestController
@RequestMapping("/api")
public class RestApiController {

    protected Producer<String,String> kafkaProducer;

    RestApiController () {
        KafkaConfig config = new KafkaConfig();
        kafkaProducer = new KafkaProducer<>(config.getKafkaProps());
    }

    @PostMapping("/send-message")
    public ResponseEntity<Void> sendMessage(@RequestBody Data data) {
        Gson gson = new Gson();
        String jsonString = gson.toJson(data);
        System.out.println(jsonString);
        return null;
    }

}
