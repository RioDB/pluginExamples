/*
				RioDBPlugin

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
 *  KAFKA is a RioDB plugin the listens as a socket server.
 *  It receives lines of text via TCP connection
 *  and stores them in a queue. 
 *  RioDB stream process can poll events from the queue. 
 *  
 *  TCP is single-threaded. Inserting data into RioDB is 
 *  so fast that it didn't make sense to spin multiple
 *  threads for accepting connections.  
 *  
 *  In the event that clients are uploading large files, 
 *  we prefer to ingest the files sequentially. So
 *  TCP is single-threaded. At least for now.  
 * 
 * 
 */

package org.riodb.plugin;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KAFKA_PRODUCER implements RioDBOutput {

	private static final String PLUGIN_TYPE = "KAFKA_PRODUCER";
	private Producer<String, String> producer;
	private String topic;
	private String key;
	private Integer partition;

	private int pluginStatus; // 0 idle; 1 started; 2 warning; 3 fatal

	Logger logger = LogManager.getLogger("co.riodb.plugin.KAFKA_PRODUCER");

	public void init(String outputParams, String[] columnHeaders) throws RioDBPluginException {

		pluginStatus = 3;

		outputParams = outputParams.replace(" - ", "-");

		String params[] = outputParams.split(" ");

		String kafkaHost;

		if (paramIndex(params, "host") >= 0 && paramIndex(params, "host") < params.length - 1) {
			kafkaHost = params[paramIndex(params, "host") + 1];
		} else {
			throw new RioDBPluginException("kafka_producer requires 'host' parameter as hostname:port");
		}

		if (paramIndex(params, "topic") >= 0 && paramIndex(params, "topic") < params.length - 1) {
			topic = params[paramIndex(params, "topic") + 1];
		} else {
			throw new RioDBPluginException("kafka_producer requires 'topic' parameter.");
		}

		if (paramIndex(params, "key") >= 0 && paramIndex(params, "key") < params.length - 1) {
			key = params[paramIndex(params, "key") + 1];
		} else {
			key = null;
		}

		if (paramIndex(params, "key") >= 0 && paramIndex(params, "key") < params.length - 1) {
			key = params[paramIndex(params, "key") + 1];

			if (paramIndex(params, "partition") >= 0 && paramIndex(params, "partition") < params.length - 1) {
				partition = Integer.valueOf(params[paramIndex(params, "partition") + 1]);
			} else {
				partition = -1;
			}

		} else {
			key = null;
		}

		Properties kafkaProps = new Properties();

		kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
		kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producer = new KafkaProducer<String, String>(kafkaProps);

		pluginStatus = 1;
	}

	@Override
	final public void send(String[] columns) {

		String output = "";
		for (String c : columns) {
			output = output + c + "\t";
		}
		output = output.substring(0, output.length() - 1); // remove last tab

		try {
			ProducerRecord<String, String> recordToSend;

			if (key != null) {
				if (partition >= 0) {
					recordToSend = new ProducerRecord<>(topic, partition, key, output);
				} else {
					recordToSend = new ProducerRecord<>(topic, key, output);
				}
			} else {
				recordToSend = new ProducerRecord<>(topic, output);
			}

			producer.send(recordToSend);

		} finally {
			producer.flush();
		}

	}

	@Override
	public RioDBPluginStatus status() {
		return new RioDBPluginStatus(pluginStatus);
	}

	@Override
	public String getType() {
		return PLUGIN_TYPE;
	}

	private int paramIndex(String[] params, String key) {
		for (int i = 0; i < params.length; i++) {
			if (params[i] != null && key != null && params[i].toLowerCase().equals(key.toLowerCase())) {
				return i;
			}
		}
		return -1;
	}

}
