package com.projects.tal.tal_kafka_v2;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
	
	public TwitterProducer() {}
	
	String consumerKey = "umfWvmqamtwiwSxNf5wWfAXU7";
	String consumerSecret = "FoIVM6brMXqDAgTQGfB5a846WfUfuYzsCXGUJTasV5S3tDOuL0";
	String token = "332207357-dt8Xgm1AduphBo1dHTCqwlqT1CLYEmRkGW9wwze0";
	String secret = "KC1B3aoVLrlF3OYRQMVQNiipEoxgPnCfsthziIIjboteS";
	
	Client hosebirdClient;
	
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

	public static void main(String[] args) {
		
		new TwitterProducer().run();

	}
	
	public void run() {
		
		// Create twitter client
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		Client client = createTwitterClient(msgQueue);
		client.connect();
		
		// Create Kafka producer 
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		// shutdown hock
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Shuting down...");
			producer.close();
		}));
		
		// Loop to send tweets to Kafka
		while (!client.isDone()) {
			  String msg = null;
			  try {
				  msg = msgQueue.poll(5, TimeUnit.SECONDS);
			  } catch (InterruptedException e) {
				  e.printStackTrace();
				  client.stop();
			  }
			  if(msg != null) {
				  logger.info(msg);
				  producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
					  public void onCompletion(RecordMetadata recordMetaDate, Exception e) {
						  if (e != null) {
							  logger.error("Something bad happened", e);
						  }
					  }
				  });
			  }
		}
	}
	
	public KafkaProducer<String, String> createKafkaProducer() {
		// Create producer properties
		String bootstrapServers = "127.0.0.1:9092";
		Properties properties = new Properties();

		// New way
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Create producer 
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		return producer;
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) { 
		// BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		//BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
		
		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		// List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList("bitcoin");
		//hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);
		
		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));
		Client hosebirdClient = builder.build();
		return hosebirdClient;

	}
}
