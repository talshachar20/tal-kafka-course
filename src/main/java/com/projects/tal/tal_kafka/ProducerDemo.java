package com.projects.tal.tal_kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {
	
	public static void main(String[] args) {
		//System.out.println("Kafka!");
		
		
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
		// send data
		producer.send(record);
		
		//In order for program not to exit before sending 
		producer.flush();
		producer.close();
	}
	


}
