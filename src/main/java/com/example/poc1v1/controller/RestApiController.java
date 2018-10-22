package com.example.poc1v1.controller;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.*;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.poc1v1.model.Data;

import com.github.jkutner.EnvKeyStore;

import java.io.File;
import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

@RestController
@RequestMapping("/api")
public class RestApiController {

    protected Producer<String,String> kafkaProducer;

    RestApiController () {

        try {

            EnvKeyStore envTrustStore = EnvKeyStore.createWithRandomPassword("KAFKA_TRUSTED_CERT");
            EnvKeyStore envKeyStore = EnvKeyStore.createWithRandomPassword("KAFKA_CLIENT_CERT_KEY", "KAFKA_CLIENT_CERT");

            File trustStore = envTrustStore.storeTemp();
            File keyStore = envKeyStore.storeTemp();

            KafkaConfig config = new KafkaConfig();
            kafkaProducer = new KafkaProducer<>(config.getKafkaProps(envTrustStore, envKeyStore, trustStore, keyStore));

        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (KeyStoreException kse) {
            throw new RuntimeException(kse);
        } catch (NoSuchAlgorithmException nsa) {
            throw new RuntimeException(nsa);
        } catch (CertificateException ce) {
            throw new RuntimeException(ce);
        }


    }

    @PostMapping("/send-message")
    public ResponseEntity<Void> sendMessage(@RequestBody Data data) {
        Gson gson = new Gson();
        String jsonString = gson.toJson(data);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("caesars", jsonString);

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
