package com.example.cassandrapoc.service;

import com.dtsx.astra.sdk.AstraDB;
import com.dtsx.astra.sdk.AstraDBCollection;
import io.stargate.sdk.data.domain.JsonDocument;
import io.stargate.sdk.data.domain.JsonDocumentResult;
import io.stargate.sdk.data.domain.CollectionDefinition;
import io.stargate.sdk.data.domain.query.UpdateQuery;
import io.stargate.sdk.data.domain.query.UpdateQueryBuilder;
import io.stargate.sdk.data.domain.query.UpdateQueryFilterBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class AstraCRUD {
    private AstraDBConnect astraDBConnect;

    public AstraCRUD(AstraDBConnect astraDBConnect) {
        this.astraDBConnect = astraDBConnect;
    }

    // Create a Collection with Name
    public void addCollection(String collectionName){

        AstraDB db = astraDBConnect.getDb();
        // define collection.
        CollectionDefinition colDefinition = CollectionDefinition.builder()
                .name(collectionName)
                .build();
        db.createCollection(colDefinition);
    }

    // Delete the Collection with Name
    public void deleteCollection(String collectionName){
        // Delete the collection
        AstraDB db = astraDBConnect.getDb();
        db.deleteCollection(collectionName);
    }

    // Add a record to the collection
    public void addRecord(String collectionName, String message){
        AstraDB db = astraDBConnect.getDb();
        AstraDBCollection collection = db.collection(collectionName);
        // Insert documents into the collection
        String id = UUID.randomUUID().toString();
        collection.insertOne(
                new JsonDocument()
                        .id(id)
                        .put("text", message)
        );
        log.info("Inserted document into " + collectionName + " collection");
    }

    // Getting all the records form the collection
    public List<JsonDocumentResult> getRecords(String collectionName){
        AstraDB db = astraDBConnect.getDb();
        AstraDBCollection collection = db.collection(collectionName);
        Stream<JsonDocumentResult> resultsSet = collection.findAll();
        return resultsSet.collect(Collectors.toList());

    }

    // Get record with id
    public Optional<JsonDocumentResult> getRecord(String collectionName, String id){
        AstraDB db = astraDBConnect.getDb();
        AstraDBCollection collection = db.collection(collectionName);
        Optional<JsonDocumentResult> resultsSet = collection.findById(id);
        return resultsSet;
    }

    // Update record with id
    public Optional<JsonDocumentResult> updateRecord(String collectionName, String id, String updatedMessage){
        AstraDB db = astraDBConnect.getDb();
        AstraDBCollection collection = db.collection(collectionName);

        Optional<JsonDocumentResult> existingRecord = collection.findById(id);
        if (existingRecord.isPresent()) {
            UpdateQueryBuilder updateQueryBuilder = new UpdateQueryBuilder();
            UpdateQueryFilterBuilder filterBuilder = updateQueryBuilder.where("_id");
            filterBuilder.isEqualsTo(id);
            updateQueryBuilder.updateSet("text", updatedMessage);
            UpdateQuery updateQuery = updateQueryBuilder.build();
            collection.updateOne(updateQuery);
            log.info("Update the record successfully");
            return getRecord(collectionName, id);
        }
        log.info("Record not found");
        return Optional.empty();
    }

    // Delete record
    public void deleteRecord(String collectionName, String id){
        AstraDB db = astraDBConnect.getDb();
        AstraDBCollection collection = db.collection(collectionName);
        int resultsSet = collection.deleteById(id);
        if (resultsSet == 1){
            log.info("Successfully deleted the record");
        } else {
            throw new RuntimeException("Failed to delete the record with ID " + id + " from collection " + collectionName);
        }
    }

}
