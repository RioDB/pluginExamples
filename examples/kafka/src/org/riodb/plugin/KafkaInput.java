/*
			KAFKA   (www.riodb.org)

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
   	KAFKA is a RioDBPlugin that can be used as INPUT or OUTPUT.
   
   	INPUT: 
   	KAFKA subscribes to a topic on a kafka server
   
	OUTPUT
	KAFKA publishes to a topic on a kafka server
	
	
	www.riodb.org
  
 */

package org.riodb.plugin;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KafkaInput {

	// access to logger
	private Logger logger = LoggerFactory.getLogger("org.riodb.plugin.KafkaInput");
	
	/*
	 * objects related to running the plugin
	 */
	// flag for breaking while() loop.
	private AtomicBoolean running = new AtomicBoolean(false);
	// if a bad message arrives (like invalid Number), we log the issue only once
	private boolean errorAlreadyCaught = false;
	// the status of this Input Plugin
	private AtomicInteger status = new AtomicInteger(0); // 0 idle; 1 started; 2 warning; 3 fatal

	/***
	 * Objects related to message fields
	 **/
	// In RioDBStreamMessageDef, an array of boolean flags specify
	// if fields (in order of array) are numeric (true) of string (false).
	private boolean numericFlags[];
	// count of numeric fields defined in stream
	private int numberFieldCount;
	// count of string fields defined in stream
	private int stringFieldCount;
	// total count of fields.
	private int totalFieldCount;
	// corresponds to the nth numeric field, or nth string field.
	private int fieldMap[];
	// map of fields names for JSON payloads.
	private HashMap<String, Integer> fieldNames;
	// default fieldDelimiter (for text/plan messages)
	private String fieldDelimiter = ",";
	// default fieldDelimiter as char (to reduce casting)
	private char fieldDelimiterChar = ','; // to avoid repetitive unboxing string...

	// format variables for timestamp
	private DateTimeFormatter timestampFormat = null;
	private int timestampFieldId = -1;
	private boolean timestampMillis = false;

	// the next 3 booleans drive the content type
	private boolean json = false;
	private boolean xml = false;
	private boolean text = false;

	/*********
	 * Objects related to Kafka
	 */
	// a place to store the kafka input properties
	private Properties kafkaInputProperties;
	// The KafkaConsumer
	// private KafkaConsumer<String, String> consumer;
	// The kafka topic to subscribe
	private String kafkaTopics;

	/*********
	 * Objects related to queueing data
	 */
	// An Inbox queue to receive stream raw data from TCP or UDP listeners
	private final LinkedBlockingQueue<String> streamPacketInbox = new LinkedBlockingQueue<String>();

	/**********
	 * Methods...
	 */

	/*
	 * getNextInputMessage
	 * 
	 * RioDB engine runs this procedure to poll messages from queue.
	 */
	public RioDBStreamMessage getNextInputMessage() throws RioDBPluginException {
		
		String message = streamPacketInbox.poll();

		if (message == null) {
			try {
				message = streamPacketInbox.take();
			} catch (InterruptedException e) {
				return null;
			}
		}

		if (text) {
			return handleTextMsg(message);// handleTextMsg(payload);
		}
		if (json) {
			return handleJsonMsg(message);
		}
		if (xml) {
			return handleXmlMsg(message);
		}

		return null;
	}

	protected int getQueueSize() {
		return streamPacketInbox.size();
	}

	protected void init(String inputParams, RioDBStreamMessageDef def) throws RioDBPluginException {

		logger.debug(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - Initializing with paramters (" + inputParams + ")");

		try {

			// GET CONFIGURABLE PARAMETERS:

			if (inputParams == null || !inputParams.contains((" "))) {

				throw new RioDBPluginException(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - requires properties, like:  INPUT KAFKA(bootstrap_servers 'localhost:9092'");

			}
			String params[] = inputParams.split(" ");
			// set topic
			kafkaTopics = getParameter(params, "topic");
			if (kafkaTopics != null && kafkaTopics.length() > 0) {
				kafkaTopics = kafkaTopics.replace("'", "");
				logger.debug(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - kafka topic is '" + kafkaTopics + "'");
			} else {
				throw new RioDBPluginException(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - requires the parameter 'topic'.");
			}

			kafkaInputProperties = new Properties();

			// get every property from the input parameters

			for (int i = 0; i < params.length - 1; i += 2) {

				String propertyName = params[i].toLowerCase().replace('_', '.');

				if (!propertyName.equals("content.type") && !propertyName.equals("delimiter")
						&& !propertyName.equals("topic")) {

					String propertyValue = params[i + 1].replace("'", "");

					if (propertyName.endsWith("deserializer")) {

						switch (propertyValue) {

						case "bytearraydeserializer":
							propertyValue = "ByteArrayDeserializer";
							break;

						case "byteBufferdeserializer":
							propertyValue = "ByteBufferDeserializer";
							break;

						case "Bytesdeserializer":
							propertyValue = "BytesDeserializer";
							break;

						case "doubledeserializer":
							propertyValue = "DoubleDeserializer";
							break;

						case "integerdeserializer":
							propertyValue = "IntegerDeserializer";
							break;

						case "longdeserializer":
							propertyValue = "LongDeserializer";
							break;

						case "stringdeserializer":
							propertyValue = "StringDeserializer";
							break;

						}

						kafkaInputProperties.put(propertyName,
								Class.forName("org.apache.kafka.common.serialization." + propertyValue));
					} else {
						kafkaInputProperties.put(propertyName, propertyValue);
					}

					logger.debug(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - kafka property '" + propertyName + "' set to '"
							+ propertyValue + "'");

				}
			}

			// set content_type for parsing...
			String contentTypeParam = getParameter(params, "content_type");
			if (contentTypeParam != null && contentTypeParam.length() > 0) {
				contentTypeParam = contentTypeParam.replace("'", "");
				if (contentTypeParam.equals("json")) {
					json = true;
				} else if (contentTypeParam.equals("text")) {
					text = true;
					// get delimiter parameter, for text contentType
					String delimiter = getParameter(params, "delimiter");
					if (delimiter != null) {
						if (delimiter.length() == 3) {
							fieldDelimiterChar = delimiter.charAt(1);
							fieldDelimiter = String.valueOf(fieldDelimiterChar);
						} else {
							status.set(3);
							throw new RioDBPluginException(
									"Syntax error. Try setting the delimiter in quotes:   DELIMITER ',' or '	' ");
						}
					}
				} else {
					throw new RioDBPluginException(
							"At this time, the only options for CONTENT_TYPE are 'json' and 'text'.");
				}
			} else {
				text = true;
				contentTypeParam = "text/plain";
			}

			// INITIALIZE LOCAL VARIABLES:

			numberFieldCount = def.getNumericFieldCount();
			stringFieldCount = def.getStringFieldCount();
			totalFieldCount = def.size();
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

			if (json) {
				fieldNames = new HashMap<String, Integer>();
				for (int i = 0; i < numericFlags.length; i++) {
					fieldNames.put(def.getFieldName(i), i);
				}
			}

			if (def.getTimestampNumericFieldId() >= 0) {
				this.timestampFieldId = def.getTimestampNumericFieldId();
				logger.debug(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - timestampNumericFieldId is " + timestampFieldId);
			}

			if (def.getTimestampFormat() != null) {
				this.timestampFormat = DateTimeFormatter.ofPattern(def.getTimestampFormat());
				logger.debug(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - timestampFormat is " + timestampFormat);
			}

			if (def.getTimestampMillis()) {
				timestampMillis = true;
			}

			logger.debug(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - Initialized.");

		} catch (ClassNotFoundException e) {
			status.set(3);
			throw new RioDBPluginException(KAFKA.PLUGIN_NAME
					+ " ClassNotFoundException while attempting to find a class for the kafka deserializer.");
		}

	}

	public void run() {

		logger.debug(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - starting for topic '" + kafkaTopics + "'");

		KafkaConsumer<String, String> consumer = null;
		try {
			consumer = new KafkaConsumer<String, String>(kafkaInputProperties);
			consumer.subscribe(Arrays.asList(kafkaTopics));

			running.set(true);
			errorAlreadyCaught = false;
			status.set(1);

			while (running.get()) {

				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				for (ConsumerRecord<String, String> record : records) {
					
					if (record.value() != null && record.value().length() > 0) {

						streamPacketInbox.offer(record.value());

					}

				}

			}

		} catch (WakeupException e) {
			if (running.get()) {
				logger.debug(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - WakeupException");
			}
		} finally {
			consumer.close();
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {

			}
		}
		

		logger.debug(KAFKA.PLUGIN_NAME + " INPUT PLUGIN -  process terminated.");

		status.set(0);

	}

	// Shutdown hook which can be called from a separate thread
	public void start() throws RioDBPluginException {
		if (!running.get()) {
			run();
		} else {
			logger.error(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - Attempted to start kafka consumer when it's already started.");
			throw new RioDBPluginException("kafka consumer is already started.");
		}
	}

	/*
	 * get status of this plugin
	 * 
	 */
	public RioDBPluginStatus status() {
		return new RioDBPluginStatus(status.get());
	}

	// Shutdown hook which can be called from a separate thread
	public void stop() {
		logger.debug(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - Stopping...");
		running.set(false);
		status.set(0);
	}

	/*
	 * handleJsonMsg
	 * 
	 * converts a json payload into a RioDBStreamMessage based on the stream message
	 * definition
	 * 
	 */
	private RioDBStreamMessage handleJsonMsg(String payload) {

		if (payload != null && payload.length() > 0) {
			payload = payload.trim();

			if (payload.startsWith("{") && payload.endsWith("}")) {
				payload = payload.substring(1, payload.length() - 1);
				// encode escaped double-quotes with some unlikely string
				payload = payload.replace("\\\"", "~^DQ");
				boolean inString = false;
				int length = payload.length();

				RioDBStreamMessage message = new RioDBStreamMessage(numberFieldCount, stringFieldCount);

				for (int i = 0; i < length; i++) {
					if (payload.charAt(i) == '"') {
						if (!inString) {
							inString = true;
						} else {
							inString = false;
							length -= 1;
						}
					} else if (inString && payload.charAt(i) == ',') {
						payload = payload.substring(0, i) + "~^CM" + payload.substring(i + 1);
						length += 3;
					} else if (inString && payload.charAt(i) == '=') {
						payload = payload.substring(0, i) + "~^EQ" + payload.substring(i + 1);
						length += 3;
					} else if (inString && payload.charAt(i) == ':') {
						payload = payload.substring(0, i) + "~^CL" + payload.substring(i + 1);
						length += 3;
					}
				}

				payload = payload.replace("\n", " "); // some unlikely string

				String properties[] = payload.split(",");
				for (int i = 0; i < properties.length; i++) {
					int colonMarker = properties[i].indexOf(':');
					if (colonMarker <= 0) {
						status.set(2);
						if (!errorAlreadyCaught) {
							logger.warn(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - Received an invalid json.");
							errorAlreadyCaught = true;
						}
						return null;
					}
					String key = properties[i].substring(1, colonMarker).trim();
					String value = properties[i].substring(colonMarker + 1).trim();
					if (key.startsWith("\"")) {
						key = key.substring(1);
					}
					if (key.endsWith("\"")) {
						key = key.substring(0, key.length() - 1);
					}
					if (value.startsWith("\"")) {
						value = value.substring(1);
					}
					if (value.endsWith("\"")) {
						value = value.substring(0, value.length() - 1);
					}

					key = key.trim().toLowerCase();

					Integer fieldIndex = fieldNames.get(key);

					if (fieldIndex != null) {
						value = value.replace("~^CL", ":").replace("~^DQ", "\\\"").replace("~^CM", ",");
						if (fieldIndex == timestampFieldId) {

							if (timestampFormat != null) {
								try {
									ZonedDateTime zdt = ZonedDateTime.parse(value, timestampFormat);
									double epoch = zdt.toInstant().toEpochMilli() / 1000;
									message.set(fieldMap[fieldIndex], epoch);
								} catch (java.time.format.DateTimeParseException e) {
									status.set(2);
									if (!errorAlreadyCaught) {
										logger.warn(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - field '" + value
												+ "' could not be parsed as '" + timestampFormat.toString() + "'");
										errorAlreadyCaught = true;
									}
									return null;
								}

							} else if (timestampMillis) {
								// can raise NumberFormatexception
								double d = (long) (Double.valueOf(value) / 1000);
								message.set(fieldMap[fieldIndex], d);
							} else {
								// can raise NumberFormatexception
								message.set(fieldMap[fieldIndex], Double.valueOf(value));
							}

						} else if (numericFlags[fieldIndex]) {
							try {
								message.set(fieldMap[fieldIndex], Double.valueOf(value));
							} catch (NumberFormatException nfe) {
								status.set(2);
								if (!errorAlreadyCaught) {
									logger.warn(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - Received an invalid number.");
									errorAlreadyCaught = true;
								}
								return null;
							}
						} else {
							message.set(fieldMap[fieldIndex], value);
						}
					}

				}

				return message;

			} else {
				System.out.println(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - Received INVALID JSON payload.");

			}
		}

		return null;
	}

	/*
	 * handleTextMsg
	 * 
	 * converts plain text message into RioDBStreamMessage
	 * 
	 */
	private RioDBStreamMessage handleTextMsg(String payload) {

		if (payload != null && payload.length() > 0) {

			payload = payload.replace("\\\"", "~^DQ");

			boolean inQuotedString = false;
			int length = payload.length();

			for (int i = 0; i < length; i++) {
				if (payload.charAt(i) == '"') {
					if (!inQuotedString) {
						inQuotedString = true;
					} else {
						inQuotedString = false;
						length -= 1;
					}
				} else if (inQuotedString && payload.charAt(i) == fieldDelimiterChar) {
					payload = payload.substring(0, i) + "~^DL" + payload.substring(i + 1);
					length += 3;
				}
			}

			RioDBStreamMessage event = new RioDBStreamMessage(numberFieldCount, stringFieldCount);
			String fields[] = payload.split(fieldDelimiter);

			if (fields.length >= totalFieldCount) {

				try {

					for (int i = 0; i < totalFieldCount; i++) {
						if (fields[i].contains("\"")) {
							fields[i] = fields[i].replace("\"", "").trim();
							if (fields[i].contains("~^")) {
								fields[i] = fields[i].replace("~^DL", fieldDelimiter).replace("~^DQ", "\"").trim();
							}
						}

						if (i == timestampFieldId) {

							if (timestampFormat != null) {
								try {
									ZonedDateTime zdt = ZonedDateTime.parse(fields[i], timestampFormat);
									double epoch = zdt.toInstant().toEpochMilli() / 1000;
									event.set(fieldMap[i], epoch);
								} catch (java.time.format.DateTimeParseException e) {
									status.set(2);
									if (!errorAlreadyCaught) {
										logger.warn(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - Field '" + fields[i]
												+ "' could not be parsed as '" + timestampFormat.toString() + "'");
										errorAlreadyCaught = true;
									}
									return null;
								}

							} else if (timestampMillis) {
								// can raise NumberFormatexception
								double d = (long) (Double.valueOf(fields[i]) / 1000);
								event.set(fieldMap[i], d);
							} else {
								// can raise NumberFormatexception
								event.set(fieldMap[i], Double.valueOf(fields[i]));
							}

						} else if (numericFlags[i]) {
							event.set(fieldMap[i], Double.valueOf(fields[i]));
						} else {
							event.set(fieldMap[i], fields[i]);
						}
					}
					return event;

				} catch (NumberFormatException nfe) {
					status.set(2);
					if (!errorAlreadyCaught) {
						logger.warn(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - Received an invalid number.");
						errorAlreadyCaught = true;
					}
					return null;
				}

			} else {
				status.set(2);
				if (!errorAlreadyCaught) {
					logger.warn(KAFKA.PLUGIN_NAME + " INPUT PLUGIN - Received fewer values than expected.");
					errorAlreadyCaught = true;
				}
			}
		}
		return null;
	}

	/*
	 * handleXmlMsg
	 * 
	 * to-be-developed. unsupported
	 * 
	 */
	private RioDBStreamMessage handleXmlMsg(String msg) {
		return null;
	}

	/*
	 * getParameter
	 *
	 * function to obtain a parameter value from an array of words For a parameter
	 * key param[i], the value is param[i+1]
	 */
	private String getParameter(String params[], String key) {
		for (int i = 0; i < params.length; i++) {
			if (key.equals(params[i].toLowerCase()) && i < params.length - 1) {
				return params[i + 1];
			}
		}
		return null;
	}

}
