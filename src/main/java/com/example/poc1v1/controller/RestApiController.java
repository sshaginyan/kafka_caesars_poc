package com.example.poc1v1.controller;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.*;
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
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("kafka-triangular-35651.caesars", jsonString);

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(exception != null)
                    exception.printStackTrace();
                else
                    System.out.println("We sent message to Kafka Topic as offset {} " + metadata.offset());
            }
        });
        return null;
    }

}
