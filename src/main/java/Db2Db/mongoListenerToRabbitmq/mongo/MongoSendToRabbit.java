package Db2Db.mongoListenerToRabbitmq.mongo;

import Db2Db.mongoListenerToRabbitmq.rabbitmq.RabbitService;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class MongoSendToRabbit {

    private RabbitService rabbitService;

    public MongoSendToRabbit(RabbitService rabbitService) {

        this.rabbitService = rabbitService;
    }

    public Map<String, Object> convertDocumentToMessage(Document document){

        // Create a dictionary to store the converted values
        Map<String, Object> dictionary = new HashMap<>();

        // Iterate through the fields of the BSON document
        for (String fieldName : document.keySet()) {
            Object value = document.get(fieldName);
            dictionary.put(fieldName, value);
        }
        return dictionary;
    }


    public void sendInjectionToQueue(ChangeStreamDocument<Document> changeEvent){
        Map<String, Object> dictionary = convertDocumentToMessage(changeEvent.getFullDocument());
        dictionary.put("operation", "insert");
        rabbitService.sendMessage(dictionary);
    }


    public void sendUpdateToQueue(ChangeStreamDocument<Document> changeEvent){
        Map<String, Object> dictionary = convertDocumentToMessage(changeEvent.getFullDocument());
        dictionary.put("operation", "update");
        rabbitService.sendMessage(dictionary);
    }

    public void sendDeleteToQueue(ChangeStreamDocument<Document> changeEvent){
        Map<String, Object> dictionary = new HashMap<>();
        dictionary.put("operation", "delete");
        dictionary.put("_id", changeEvent.getDocumentKey().get("_id"));
        rabbitService.sendMessage(dictionary);
    }
}
