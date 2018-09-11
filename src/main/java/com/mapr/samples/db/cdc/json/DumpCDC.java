package com.mapr.samples.db.cdc.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.ojai.*;
import org.ojai.store.cdc.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class DumpCDC {

  private static void insertAndUpdateDocument(ChangeDataRecord changeDataRecord) {
    String docId = changeDataRecord.getId().getString();

    // Use the ChangeNode Iterator to capture all the individual changes
    Iterator<KeyValue<FieldPath, ChangeNode>> cdrItr = changeDataRecord.iterator();
    while (cdrItr.hasNext()) {
      Map.Entry<FieldPath, ChangeNode> changeNodeEntry = cdrItr.next();
      String fieldPathAsString = changeNodeEntry.getKey().asPathString();
      ChangeNode changeNode = changeNodeEntry.getValue();

      // When "INSERTING" a documen the field path is empty (new document)
      // and all the changes are made in a single object represented as a Map
      if (fieldPathAsString == null || fieldPathAsString.equals("")) { // Insert

        // get the value from the ChangeNode
        // and check if the fields firstName, LastName or Address are part of the operation
        Map<String, Object> documentInserted = changeNode.getMap();
        System.out.println("  \"insertData\" : {");
        int i = 0;
        for (Map.Entry<String, Object> entry : documentInserted.entrySet()) {
          if (i != 0) {
            System.out.println(",");
          }
          System.out.print("    \"" + entry.getKey() + " : " + entry.getValue().toString());
          i++;
        }
        System.out.println("  }");


      } else {
        System.out.println("  \"updateData\" : {");
        switch(changeNode.getType()) {
          case NULL:
            System.out.println("    " + fieldPathAsString + " : NULL");
            break;
          case BOOLEAN:
            System.out.println("    " + fieldPathAsString + " : " + changeNode.getBoolean());
            break;
          case STRING:
            System.out.println("    " + fieldPathAsString + " : " + changeNode.getString());
            break;
          case SHORT:
            System.out.println("    " + fieldPathAsString + " : " + changeNode.getShort());
            break;
          case BYTE:
            System.out.println("    " + fieldPathAsString + " : " + changeNode.getByte());
            break;
          case INT:
            System.out.println("    " + fieldPathAsString + " : " + changeNode.getInt());
            break;
          case LONG:
            System.out.println("    " + fieldPathAsString + " : " + changeNode.getLong());
            break;
          case FLOAT:
            System.out.println("    " + fieldPathAsString + " : " + changeNode.getFloat());
            break;
          case DOUBLE:
            System.out.println("    " + fieldPathAsString + " : " + changeNode.getDouble());
            break;
          case DECIMAL:
            System.out.println("    " + fieldPathAsString + " : " + changeNode.getDecimal());
            break;
          case DATE:
            System.out.println("    " + fieldPathAsString + " : " + changeNode.getDate());
            break;
          case TIME:
            System.out.println("    " + fieldPathAsString + " : " + changeNode.getTime());
            break;
          case TIMESTAMP:
            System.out.println("    " + fieldPathAsString + " : " + changeNode.getTimestamp());
            break;
          case INTERVAL:
            System.out.println("    " + fieldPathAsString + " : " + changeNode.getInterval());
            break;
          default:
            break;
        }

        System.out.println(" }");
      }


    }

  }



  @SuppressWarnings("Duplicates")
  public static void main(String[] args) {
    if (args.length != 2) {
      System.out.println("com.mapr.samples.db.cdc.json.DumpCDC <stream path> <topic>");
      System.exit(1);
    }
    String streamPath = args[0];
    String topicName  = args[1];
    String ChangeLog  = streamPath + ":" + topicName;

    System.out.println("==== Start DumpCDC with " + ChangeLog + "===");


    // Consumer configuration
    Properties consumerProperties = new Properties();
    consumerProperties.setProperty("group.id", "cdc.consumer.demo_table.json.fts_geo");
    consumerProperties.setProperty("enable.auto.commit", "true");
    consumerProperties.setProperty("auto.offset.reset", "latest");
    consumerProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProperties.setProperty("value.deserializer", "com.mapr.db.cdc.ChangeDataRecordDeserializer");


    // Consumer used to consume MapR-DB CDC events
    KafkaConsumer<byte[], ChangeDataRecord> consumer = new KafkaConsumer<byte[], ChangeDataRecord>(consumerProperties);
    consumer.subscribe(Arrays.asList(ChangeLog));

    while (true) {
      ConsumerRecords<byte[], ChangeDataRecord> changeRecords = consumer.poll(500);
      Iterator<ConsumerRecord<byte[], ChangeDataRecord>> iter = changeRecords.iterator();

      while (iter.hasNext()) {
        ConsumerRecord<byte[], ChangeDataRecord> crec = iter.next();
        ChangeDataRecord changeDataRecord = crec.value();
        String documentId = changeDataRecord.getId().getString();

        System.out.println("{");
		System.out.println("  \"type\" : " + changeDataRecord.getType() + ",");
		System.out.println("  \"documentId : " + documentId + ",");
		System.out.println("  \"recordOpTime : " + String.valueOf(changeDataRecord.getOpTimestamp()) + ",");
		System.out.println("  \"recordServerOpTime : " + String.valueOf(changeDataRecord.getServerTimestamp()) + ",");
        if (changeDataRecord.getType() == ChangeDataRecordType.RECORD_INSERT) {
          insertAndUpdateDocument(changeDataRecord);
        } else if (changeDataRecord.getType() == ChangeDataRecordType.RECORD_UPDATE) {
          insertAndUpdateDocument(changeDataRecord);
        }
        System.out.println("}");
      }
    }
  }


}
