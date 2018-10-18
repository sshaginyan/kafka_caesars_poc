package com.example.poc1v1.controller;

import com.google.gson.Gson;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.poc1v1.model.Data;

@RestController
@RequestMapping("/api")
public class RestApiController {

    @PostMapping("/send-message")
    public ResponseEntity<Void> sendMessage(@RequestBody Data data) {
        Gson gson = new Gson();
        String jsonString = gson.toJson(data);
        System.out.println(jsonString);
        return null;
    }

}
