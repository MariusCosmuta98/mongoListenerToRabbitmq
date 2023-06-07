package Db2Db.mongoListenerToRabbitmq.mongo;


import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;


// Mongo configuration and listen new streams
@Component
public class MongoConfig {
    private final MongoSendToRabbit mongoSendToRabbit;
    @Value("${mongodb.primary.uri}")
    private String mongodbUri;

    @Value("${mongodb.primary.database}")
    private String primaryDatabase;

    @Value("${mongodb.primary.collection}")
    private String primaryCollection;

    public MongoConfig(MongoSendToRabbit mongoSendToRabbit) {

        this.mongoSendToRabbit = mongoSendToRabbit;
    }

    @Bean
    public void changeStream() {

        // connect to the local database server
        MongoClient mongoClient = MongoClients.create(mongodbUri);

        // Select the MongoDB database
        MongoDatabase database = mongoClient.getDatabase(primaryDatabase);

        // Select the collection to query
        MongoCollection<Document> collection = database.getCollection(primaryCollection);

        // Create pipeline for operationType filter
        List<Bson> pipeline = List.of(
                Aggregates.match(
                        Filters.in(
                                "operationType",
                                Arrays.asList("insert", "update", "delete")
                        )));

        // Create the Change Stream
        ChangeStreamIterable<Document> changeStream = collection.watch(pipeline)
                .fullDocument(FullDocument.UPDATE_LOOKUP);

        // Iterate over the Change Stream
        for (ChangeStreamDocument<Document> changeEvent : changeStream) {
            switch (changeEvent.getOperationType()) {
                case INSERT -> mongoSendToRabbit.sendInjectionToQueue(changeEvent);
                case UPDATE -> mongoSendToRabbit.sendUpdateToQueue(changeEvent);
                case DELETE -> mongoSendToRabbit.sendDeleteToQueue(changeEvent);
            }
        }
    }
}
