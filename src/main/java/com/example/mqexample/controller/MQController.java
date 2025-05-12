package com.example.mqexample.controller;

import com.example.mqexample.model.MyMessage;
import com.example.mqexample.service.MQService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/mq")
public class MQController {

    @Autowired
    private MQService mqService;

    @PostMapping("/send")
    public String sendMessage(@RequestBody MyMessage message) {
        try {
            mqService.sendMessage(message);
            return "Message sent successfully!";
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    @GetMapping("/receive")
    public String receiveMessage() {
        try {
            return mqService.receiveMessage();
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    @GetMapping("/receiveById/{id}")
    public List<MyMessage> receiveMessagesById(@PathVariable String id) {
        try {
            return mqService.receiveMessagesById(id);
        } catch (Exception e) {
            throw new RuntimeException("Error retrieving messages: " + e.getMessage());
        }
    }

    @PutMapping("/update/{id}")
    public String updateMessagesById(@PathVariable String id, @RequestBody MyMessage updatedMessage) {
        try {
            mqService.updateMessagesById(id, updatedMessage);
            return "Messages updated successfully!";
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    @DeleteMapping("/delete/{id}")
    public String deleteMessagesById(@PathVariable String id) {
        try {
            mqService.deleteMessagesById(id);
            return "Messages deleted successfully!";
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    @GetMapping("/receiveAll")
    public List<MyMessage> receiveAllMessages() {
        try {
            return mqService.receiveAllMessages();
        } catch (Exception e) {
            throw new RuntimeException("Error retrieving all messages: " + e.getMessage());
        }
    }
}