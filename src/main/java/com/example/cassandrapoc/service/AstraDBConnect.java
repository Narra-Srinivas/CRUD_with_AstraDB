package com.example.cassandrapoc.service;

import com.dtsx.astra.sdk.AstraDB;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AstraDBConnect {
    private AstraDB db;
    public AstraDB getDb() {
        return db;
    }

    public AstraDBConnect() {
        String astraToken = ("AstraCS:qzCDUJNWpGBICUejMamqZQIr:5f9b635424d135af80454c1649aa09d4877b956227bb13deb3061400d05e2125");
        String astraApiEndpoint = ("https://0e9a2759-9b51-4d76-b2e2-8b883ed10c44-us-east-1.apps.astra.datastax.com");
        // Initialize the client.
        log.info("Initializing AstraDB");
        this.db = new AstraDB(astraToken, astraApiEndpoint);
        log.info("Connected to AstraDB");
    }

}
