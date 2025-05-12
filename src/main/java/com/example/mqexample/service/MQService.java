package com.example.mqexample.service;

import com.example.mqexample.model.MyMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class MQService {

    @Value("${ibm.mq.queueManager}")
    private String queueManagerName;

    @Value("${ibm.mq.channel}")
    private String channel;

    @Value("${ibm.mq.host}")
    private String host;

    @Value("${ibm.mq.port}")
    private int port;

    @Value("${ibm.mq.queue}")
    private String queueName;

    @Value("${ibm.mq.user}")
    private String user;

    @Value("${ibm.mq.password}")
    private String password;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void sendMessage(Object messageObj) throws MQException, IOException, JsonProcessingException {
        String messageText = objectMapper.writeValueAsString(messageObj);
        MQQueueManager queueManager = new MQQueueManager(queueManagerName);
        MQQueue queue = queueManager.accessQueue(queueName, MQConstants.MQOO_OUTPUT);
        MQMessage message = new MQMessage();
        message.writeString(messageText);
        queue.put(message);
        queue.close();
        queueManager.disconnect();
    }

    public String receiveMessage() throws MQException, IOException {
        MQQueueManager queueManager = new MQQueueManager(queueManagerName);
        MQQueue queue = queueManager.accessQueue(queueName, MQConstants.MQOO_INPUT_SHARED);
        MQMessage message = new MQMessage();
        queue.get(message);
        String receivedMessage = message.readString(message.getMessageLength());
        queue.close();
        queueManager.disconnect();
        return receivedMessage;
    }

    public List<MyMessage> receiveMessagesById(String id) throws MQException, IOException {
        List<MyMessage> filteredMessages = new ArrayList<>();
        List<byte[]> messageIdsToRemove = new ArrayList<>();
        MQQueueManager queueManager = new MQQueueManager(queueManagerName);
        MQQueue browseQueue = queueManager.accessQueue(queueName, MQConstants.MQOO_BROWSE);

        try {

            MQGetMessageOptions browseGmo = new MQGetMessageOptions();
            browseGmo.options = MQConstants.MQGMO_BROWSE_NEXT;

            MQMessage message = new MQMessage();
            boolean hasMoreMessages = true;

            while (hasMoreMessages) {
                try {
                    browseQueue.get(message, browseGmo);
                    String messageText = message.readString(message.getMessageLength());
                    MyMessage myMessage = objectMapper.readValue(messageText, MyMessage.class);

                    if (id.equals(myMessage.getId())) {
                        filteredMessages.add(myMessage);
                        messageIdsToRemove.add(message.messageId); 
                    }

                    message = new MQMessage();
                } catch (MQException e) {
                    if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
                        hasMoreMessages = false; 
                    } else {
                        throw e;
                    }
                }
            }

            // إغلاق browseQueue
            browseQueue.close();
        } finally {
            browseQueue.close();
        }

        if (!messageIdsToRemove.isEmpty()) {
            MQQueue removeQueue = queueManager.accessQueue(queueName, MQConstants.MQOO_INPUT_SHARED);
            try {
                MQGetMessageOptions removeGmo = new MQGetMessageOptions();
                removeGmo.options = MQConstants.MQGMO_NO_WAIT;
                removeGmo.matchOptions = MQConstants.MQMO_MATCH_MSG_ID;

                for (byte[] messageId : messageIdsToRemove) {
                    MQMessage removeMessage = new MQMessage();
                    removeMessage.messageId = messageId;
                    try {
                        removeQueue.get(removeMessage, removeGmo); 
                    } catch (MQException e) {
                        if (e.reasonCode != MQConstants.MQRC_NO_MSG_AVAILABLE) {
                            throw e;
                        }
                    }
                }
            } finally {
                removeQueue.close();
            }
        }

        queueManager.disconnect();
        return filteredMessages;
    }

    public void updateMessagesById(String id, MyMessage updatedMessage) throws MQException, IOException, JsonProcessingException {
        List<byte[]> messageIdsToRemove = new ArrayList<>();
        MQQueueManager queueManager = new MQQueueManager(queueManagerName);
        MQQueue browseQueue = queueManager.accessQueue(queueName, MQConstants.MQOO_BROWSE);

        try {

            MQGetMessageOptions browseGmo = new MQGetMessageOptions();
            browseGmo.options = MQConstants.MQGMO_BROWSE_NEXT;


            MQMessage message = new MQMessage();
            boolean hasMoreMessages = true;


            while (hasMoreMessages) {
                try {
                    browseQueue.get(message, browseGmo);
                    String messageText = message.readString(message.getMessageLength());
                    MyMessage myMessage = objectMapper.readValue(messageText, MyMessage.class);


                    if (id.equals(myMessage.getId())) {
                        messageIdsToRemove.add(message.messageId); 
                    }


                    message = new MQMessage();
                } catch (MQException e) {
                    if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
                        hasMoreMessages = false; 
                    } else {
                        throw e;
                    }
                }
            }


            browseQueue.close();
        } finally {
            browseQueue.close();
        }


        if (!messageIdsToRemove.isEmpty()) {
            MQQueue removeQueue = queueManager.accessQueue(queueName, MQConstants.MQOO_INPUT_SHARED);
            try {
                MQGetMessageOptions removeGmo = new MQGetMessageOptions();
                removeGmo.options = MQConstants.MQGMO_NO_WAIT;
                removeGmo.matchOptions = MQConstants.MQMO_MATCH_MSG_ID;

                for (byte[] messageId : messageIdsToRemove) {
                    MQMessage removeMessage = new MQMessage();
                    removeMessage.messageId = messageId;
                    try {
                        removeQueue.get(removeMessage, removeGmo); 
                    } catch (MQException e) {
                        if (e.reasonCode != MQConstants.MQRC_NO_MSG_AVAILABLE) {
                            throw e;
                        }
                    }
                }
            } finally {
                removeQueue.close();
            }
        }

        updatedMessage.setId(id); 
        sendMessage(updatedMessage);

        queueManager.disconnect();
    }

    public void deleteMessagesById(String id) throws MQException, IOException {
        List<byte[]> messageIdsToRemove = new ArrayList<>();
        MQQueueManager queueManager = new MQQueueManager(queueManagerName);
        MQQueue browseQueue = queueManager.accessQueue(queueName, MQConstants.MQOO_BROWSE);

        try {
            MQGetMessageOptions browseGmo = new MQGetMessageOptions();
            browseGmo.options = MQConstants.MQGMO_BROWSE_NEXT;

            MQMessage message = new MQMessage();
            boolean hasMoreMessages = true;

            while (hasMoreMessages) {
                try {
                    browseQueue.get(message, browseGmo);
                    String messageText = message.readString(message.getMessageLength());
                    MyMessage myMessage = objectMapper.readValue(messageText, MyMessage.class);

                    if (id.equals(myMessage.getId())) {
                        messageIdsToRemove.add(message.messageId); 
                    }

                    message = new MQMessage();
                } catch (MQException e) {
                    if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
                        hasMoreMessages = false; 
                    } else {
                        throw e;
                    }
                }
            }

            browseQueue.close();
        } finally {
            browseQueue.close();
        }

        if (!messageIdsToRemove.isEmpty()) {
            MQQueue removeQueue = queueManager.accessQueue(queueName, MQConstants.MQOO_INPUT_SHARED);
            try {
                MQGetMessageOptions removeGmo = new MQGetMessageOptions();
                removeGmo.options = MQConstants.MQGMO_NO_WAIT;
                removeGmo.matchOptions = MQConstants.MQMO_MATCH_MSG_ID;

                for (byte[] messageId : messageIdsToRemove) {
                    MQMessage removeMessage = new MQMessage();
                    removeMessage.messageId = messageId;
                    try {
                        removeQueue.get(removeMessage, removeGmo); 
                    } catch (MQException e) {
                        if (e.reasonCode != MQConstants.MQRC_NO_MSG_AVAILABLE) {
                            throw e;
                        }
                    }
                }
            } finally {
                removeQueue.close();
            }
        }

        queueManager.disconnect();
    }

    public List<MyMessage> receiveAllMessages() throws MQException, IOException {
        List<MyMessage> allMessages = new ArrayList<>();
        MQQueueManager queueManager = new MQQueueManager(queueManagerName);
        MQQueue browseQueue = queueManager.accessQueue(queueName, MQConstants.MQOO_BROWSE);

        try {

            MQGetMessageOptions browseGmo = new MQGetMessageOptions();
            browseGmo.options = MQConstants.MQGMO_BROWSE_NEXT;

            MQMessage message = new MQMessage();
            boolean hasMoreMessages = true;

            while (hasMoreMessages) {
                try {
                    browseQueue.get(message, browseGmo);
                    String messageText = message.readString(message.getMessageLength());
                    MyMessage myMessage = objectMapper.readValue(messageText, MyMessage.class);
                    allMessages.add(myMessage);

                    message = new MQMessage();
                } catch (MQException e) {
                    if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
                        hasMoreMessages = false; 
                    } else {
                        throw e;
                    }
                }
            }
        } finally {
            browseQueue.close();
            queueManager.disconnect();
        }

        return allMessages;
    }
}