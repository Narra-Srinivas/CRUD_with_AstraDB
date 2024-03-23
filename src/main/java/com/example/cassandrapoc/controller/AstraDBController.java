package com.example.cassandrapoc.controller;

import com.example.cassandrapoc.model.Collection;
import com.example.cassandrapoc.service.AstraCRUD;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/v1")
public class AstraDBController {
    private final AstraCRUD astraCRUD;

    @Autowired
    public AstraDBController(AstraCRUD astraCRUD) {
        this.astraCRUD = astraCRUD;
    }

    @PostMapping("/createCollection")
    public ResponseEntity<String> createCollection(@RequestBody Collection collection){
        log.info("Trying to create collection with name " + collection.getCollectionName());
        try {
            astraCRUD.addCollection(collection.getCollectionName());
            return ResponseEntity.status(HttpStatus.CREATED)
                    .body("Created Collection with name: " + collection.getCollectionName());
        } catch (Exception e){
            log.error("Error while creating collection with name " + collection.getCollectionName() +": " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage());
        }
    }

    @DeleteMapping("/delete")
    public ResponseEntity<String> deleteCollection(@RequestBody Collection collection) {
        log.info("Trying to Delete Collection " + collection.getCollectionName());
        try{
            astraCRUD.deleteCollection(collection.getCollectionName());
            return ResponseEntity.ok("Deleted Collection: " + collection.getCollectionName());
        } catch (Exception e){
            log.error("Error while deleting " + collection.getCollectionName() + " Collection: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage());
        }
    }

}
