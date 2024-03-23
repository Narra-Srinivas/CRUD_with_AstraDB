package com.example.cassandrapoc.controller;

import com.example.cassandrapoc.model.Message;
import com.example.cassandrapoc.service.AstraCRUD;
import io.stargate.sdk.data.domain.JsonDocumentResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.HttpStatus;

import java.util.List;
import java.util.Optional;


@Slf4j
@RestController
@RequestMapping("/api/v1")
public class CollectionController {
    private final AstraCRUD astraCRUD;

    @Autowired
    public CollectionController(AstraCRUD astraCRUD) {
        this.astraCRUD = astraCRUD;
    }

    @GetMapping("/{collectionName}")
    public ResponseEntity<?> getMessages(@PathVariable String collectionName){
        log.info("Fetching all the records from "+ collectionName);
        try{
            List<JsonDocumentResult> messages = astraCRUD.getRecords(collectionName);
            return ResponseEntity.ok(messages);
        } catch (Exception e){
            log.error("Error while retrieving records from " + collectionName +": " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error while retrieving records from " + collectionName +": " + e.getMessage());
        }

    }

    @GetMapping("/{collectionName}/{id}")
    public ResponseEntity<?> getMessage(@PathVariable String collectionName, @PathVariable String id){
        log.info("Fetching the records from "+ collectionName);
        try{
            Optional<JsonDocumentResult> message = astraCRUD.getRecord(collectionName, id);
            return ResponseEntity.ok(message);
        } catch (Exception e){
            log.error("Error while retrieving record from " + collectionName +": " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error while retrieving record from " + collectionName +": " + e.getMessage());
        }
    }

    @PostMapping("/{collectionName}/add")
    public ResponseEntity<String> addMessage(@RequestBody Message message, @PathVariable String collectionName){
        log.info("Trying to insert record into " + collectionName);
        try{
            astraCRUD.addRecord(collectionName, message.getMessage());
            return ResponseEntity.status(HttpStatus.CREATED)
                    .body("Inserted record into " + collectionName + " successfully");
        } catch (Exception e){
            log.error("Error while adding record into " + collectionName +": " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage());
        }
    }

    @PutMapping("/update/{collectionName}/{id}")
    public ResponseEntity<?> updateMessage(@PathVariable String id, @PathVariable String collectionName, @RequestBody Message message){
        log.info("Attempting to update the record");
        try {
            Optional<JsonDocumentResult> updatedRecord = astraCRUD.updateRecord(collectionName, id, message.getMessage());
            return ResponseEntity.ok("Updated record with id: "+ id + " successfully");
        } catch(Exception e) {
            log.error("Error occurred while updating the record");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error" + e.getMessage());
        }
    }

    @DeleteMapping("/delete/{collectionName}/{id}")
    public ResponseEntity<?> deleteMessage(@PathVariable String id, @PathVariable String collectionName){
        log.info("Trying to delete the record by ID");
        try{
            astraCRUD.deleteRecord(collectionName, id);
            return ResponseEntity.ok("Record with id: " + id + " deleted successfully");
        } catch (Exception e) {
            log.error("Error while deleting the record with id: "+id);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage());
        }
    }

}
