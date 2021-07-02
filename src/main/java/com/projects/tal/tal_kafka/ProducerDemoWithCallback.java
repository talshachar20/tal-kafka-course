package com.projects.tal.tal_kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
	
	public static void main(String[] args) {
		//System.out.println("Kafka!");
		
		Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
		
		
		// Create producer properties
		String bootstrapServers = "127.0.0.1:9092";
		Properties properties = new Properties();
		
		// Old way
		//properties.setProperty("bootstrap.servers", bootstrapServers);
		//properties.setProperty("key.serialzer", StringSerializer.class.getName());
		//properties.setProperty("value.serialzer", StringSerializer.class.getName());
		
		// New way
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Create producer 
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		// create a producer record
		ProducerRecord<String, String> record =
				new ProducerRecord<String, String>("tal", "tal !!");
		// send data - (with call back) 
		producer.send(record, new Callback() {
			
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// this block will be execute on sent or exception
				if(exception == null) {
					logger.info("Recieved new metadata \n" +
					"Topic: " + metadata.topic() + " \n" +
					"Partition: " + metadata.partition() + " \n" +
					"Offset: " + metadata.offset() + " \n" 	+
					"Timestamp: " + metadata.timestamp() + " \n" 	);
				} else {
					logger.error(" Error: " + exception);
				}
				
			}
		});
		
		//In order for program not to exit before sending 
		producer.flush();
		producer.close();
	}
	


}
