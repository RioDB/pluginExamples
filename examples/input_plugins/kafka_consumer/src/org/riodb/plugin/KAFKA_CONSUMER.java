/*
		KAFKA_CONSUMER (www.riodb.org)

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
 
*/


/*
 * 
 *  KAFKA_CONSUMER is a RioDB plugin that consumes data from a Kafka topic
 *  It dequeues messages from the Kafka topic and stores them in a queue. 
 *  RioDB stream process can poll messages from the queue. 
 *  
 *  KAFKA_CONSUMER starts a new single-threaded. 
 *   
 */

package org.riodb.plugin;

import java.time.Duration;
//import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jctools.queues.SpscChunkedArrayQueue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.common.TopicPartition;

public class KAFKA_CONSUMER implements RioDBDataSource, Runnable {

	private static final String PLUGIN_TYPE = "KAFKA_CONSUMER";

	private static final int QUEUE_INIT_CAPACITY = 244; // 10000;
	private static final int MAX_CAPACITY = 1000000;
	private static final int KAFKA_POLL_DURATION = 100;
	private static Logger logger = LogManager.getLogger(KAFKA_CONSUMER.class.getName());

	private String kafkaHost;
	private String kafkaGroup;
	private String kafkaTopic;
	private Properties kafkaProperties;

	private int status = 0; // 0 idle; 1 started; 2 warning; 3 fatal

	// local copy of Stream mapping of which fields are number.
	private boolean numericFlags[];
	private int numberFieldCount;
	private int stringFieldCount;
	private int totalFieldCount;
	private int fieldMap[];

	// An Inbox queue to receive Strings from the TCP socket
	private SpscChunkedArrayQueue<String> inboxQueue = new SpscChunkedArrayQueue<String>(QUEUE_INIT_CAPACITY,
			MAX_CAPACITY);

	private Thread kafkConsumerThread;

	private boolean interrupt;
	private boolean errorAlreadyCaught = false;

	@Override
	public RioDBStreamEvent getNextEvent() throws RioDBPluginException {

		String s = inboxQueue.poll();
		
		
		if (s != null && s.length() > 0) {
			
						
			s = s.trim();

			RioDBStreamEvent event = new RioDBStreamEvent(numberFieldCount, stringFieldCount);
			String fields[] = s.split("\t");

			if (fields.length >= totalFieldCount) {
				int numCounter = 0;
				int strCounter = 0;

				for (int i = 0; i < fields.length; i++) {
					if (numericFlags[i]) {
						try {
							event.set(numCounter++, Double.valueOf(fields[i]));
						} catch (NumberFormatException nfe) {
							status = 2;
							if (!errorAlreadyCaught) {
								logger.warn(PLUGIN_TYPE + " received INVALID NUMBER [" + s + "]");
								errorAlreadyCaught = true;
								return null;
							}
						}
					} else {
						event.set(strCounter++, fields[i]);
					}
				}
				return event;
			} else {
				status = 2;
				if (!errorAlreadyCaught) {
					logger.warn(PLUGIN_TYPE + " received fewer values than expected. [" + s + "]");
					errorAlreadyCaught = true;
				}
			}
		}
		return null;
	}

	@Override
	public int getQueueSize() {
		return inboxQueue.size();
	}

	@Override
	public String getType() {
		return PLUGIN_TYPE;
	}

	@Override
	public void init(String configParams, RioDBStreamEventDef def) throws RioDBPluginException {

		logger.info("initializing " + PLUGIN_TYPE + " plugin with paramters (" + configParams + ")");

		numberFieldCount = def.getNumericFieldCount();
		stringFieldCount = def.getStringFieldCount();
		totalFieldCount = numberFieldCount + stringFieldCount;
		numericFlags = def.getAllNumericFlags();
		int numericCounter = 0;
		int stringCounter = 0;

		fieldMap = new int[numericFlags.length];
		for (int i = 0; i < numericFlags.length; i++) {
			if (numericFlags[i]) {
				fieldMap[i] = numericCounter++;
			} else {
				fieldMap[i] = stringCounter++;
			}
		}

		configParams = configParams.replace(" - ", "-");
		String params[] = configParams.split(" ");
		if (params.length < 2)
			throw new RioDBPluginException("KAFKA_CONSUMER requires parameter 'host' as hostname:port");

		if (paramIndex(params, "host") >= 0 && paramIndex(params, "host") < params.length - 1) {
			kafkaHost = params[paramIndex(params, "host") + 1];
		} else {
			throw new RioDBPluginException("kafka_producer requires 'host' parameter.");
		}

		if (paramIndex(params, "group") >= 0 && paramIndex(params, "group") < params.length - 1) {
			kafkaGroup = params[paramIndex(params, "group") + 1];
		} else {
			throw new RioDBPluginException("kafka_producer requires 'group' parameter.");
		}

		if (paramIndex(params, "topic") >= 0 && paramIndex(params, "topic") < params.length - 1) {
			kafkaTopic = params[paramIndex(params, "topic") + 1];
		} else {
			throw new RioDBPluginException("kafka_producer requires 'topic' parameter.");
		}

		kafkaProperties = new Properties();
		kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
		kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroup);
		kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		status = 0;

	}

	private int paramIndex(String[] params, String key) {
		for (int i = 0; i < params.length; i++) {
			if (params[i] != null && key != null && params[i].toLowerCase().equals(key.toLowerCase())) {
				return i;
			}
		}
		return -1;
	}

	public void run() {

		logger.info("Starting Plugin " + PLUGIN_TYPE);

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties);
		try {

			// TestConsumerRebalanceListener rebalanceListener = new
			// TestConsumerRebalanceListener();
			// consumer.subscribe(Collections.singletonList(kafkaTopic), rebalanceListener);
			consumer.subscribe(Collections.singletonList(kafkaTopic));

			while (!interrupt) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(KAFKA_POLL_DURATION));
				for (ConsumerRecord<String, String> record : records) {
					if(record.value() != null)
						inboxQueue.offer(record.value());
				}

				consumer.commitSync();

			}
		} finally {
			consumer.close();
		}

		status = 0;

		logger.info("Plugin " + PLUGIN_TYPE + " stopped.");
	}

	@Override
	public void start() throws RioDBPluginException {
		interrupt = false;
		kafkConsumerThread = new Thread(this);
		kafkConsumerThread.setName("KAFKA_CONSUMER_THREAD");
		kafkConsumerThread.start();
		status = 1;
	}

	public RioDBPluginStatus status() {
		return new RioDBPluginStatus(status);
	}

	@Override
	public void stop() {
		interrupt = true;
		kafkConsumerThread.interrupt();
		status = 0;
	}
	/*
	 * private static class TestConsumerRebalanceListener implements
	 * ConsumerRebalanceListener {
	 * 
	 * @Override public void onPartitionsRevoked(Collection<TopicPartition>
	 * partitions) {
	 * System.out.println("Called onPartitionsRevoked with partitions:" +
	 * partitions); }
	 * 
	 * @Override public void onPartitionsAssigned(Collection<TopicPartition>
	 * partitions) {
	 * System.out.println("Called onPartitionsAssigned with partitions:" +
	 * partitions); }
	 * 
	 * }
	 */
}