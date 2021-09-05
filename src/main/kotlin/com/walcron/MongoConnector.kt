package com.walcron

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters.eq
import org.bson.Document
import org.bson.conversions.Bson
import java.util.*
import kotlin.system.exitProcess


object MongoConnector {
    var connectStr: String

    init {
        val props:Properties = Properties()
        val resource = this::class.java.classLoader.getResourceAsStream("connector.properties")
        resource.use {
            props.load(resource)
            connectStr = props.getProperty("connection-string")
            println("Connection $connectStr")
        }
    }

    fun connect() {
        try{
            val connectionString = ConnectionString(connectStr)
            val settings: MongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build()
            val mongoClient:MongoClient = MongoClients.create(settings)
            showDatabase(mongoClient)
            val database = mongoClient.getDatabase(requestDBToConnect())
            showCollections(database)
            runCollection(database)
        } catch(e:Error) {
            e.printStackTrace()
        }

    }

    private fun runCollection(database: MongoDatabase) {
        while(true) {
            val collection = database.getCollection(requestCollectionToConnect())
            val filterField = requestFilter()
            val findField = if(filterField.first == null) filterField.second else filterField.first
            if(findField != null) {
                val collections = collection.find(findField)
                collections.take(10).forEach(::println);
            }
            println("--Done--")
        }
    }

    private fun requestFilter(): Pair<Document?, Bson?> {
        print("Query field:")
        val scanner = Scanner(System.`in`)
        val field = scanner.nextLine()
        return if (field.isEmpty()) {
            Pair(Document(), null)
        } else {
            print("Query Value:")
            val value = scanner.nextLine()
            Pair(null, eq(field, value))
        }
    }

    private fun requestCollectionToConnect(): String {
        print("Provide Collection To Connect:")
        val scanner = Scanner(System.`in`)
        val readLine = scanner.nextLine()
        if(readLine.isEmpty()) {
            exitProcess(0)
        }
        return readLine
    }

    private fun showCollections(database: MongoDatabase) {
        println("-----")
        database.listCollectionNames().forEach(::println)
        println("-----")
    }

    private fun requestDBToConnect(): String {
        print("Provide DB To Connect:")
        val scanner = Scanner(System.`in`)
        return scanner.nextLine()

    }

    private fun showDatabase(mongoClient: MongoClient) {
        println("Available databases:")
        println("----")
        mongoClient.listDatabases().forEach(::println)
        println("----")
    }
}