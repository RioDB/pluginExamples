/*-
 * Copyright (c) 2025 Lucio D Matos - www.riodb.co
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package co.riodb.plugin.input;

import co.riodb.plugin.resources.LoggingService;
import co.riodb.plugin.resources.PluginUtils;
import co.riodb.plugin.resources.RioDBInputPlugin;
import co.riodb.plugin.resources.RioDBPluginException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jctools.queues.SpscChunkedArrayQueue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KAFKA extends RioDBInputPlugin implements Runnable {

	// default queue limits
	public static final int DEFAULT_QUEUE_INIT_CAPACITY = 244; // 10000;

	public static final int DEFAULT_MAX_CAPACITY = 1000000;
	public static final int DEFAULT_CONN_BACKLOG = 1000;
	private String kafkaUrl;

	private String kafkaTopic;
	private String kafkaGroup;

	// running in mode EFFICIENT vs EXTREME
	private boolean extremeMode = false;

	// Character set
	// TODO: implement character set
	private Charset charSet;

	// An Inbox queue based on arrayQueue for extreme loads
	private SpscChunkedArrayQueue<String> streamArrayQueue;

	// An index queue based on blockingQueue for efficient processing
	private LinkedBlockingQueue<String> streamBlockingQueue;

	// runnable thread
	private Thread kafkaClientThread;

	// process flags.
	private AtomicBoolean interrupt;

	private AtomicBoolean errorAlreadyCaught = new AtomicBoolean(false);

	public KAFKA(LoggingService loggingService) {
		super(loggingService);
	}

	public void clearWarnings() {
		super.clearWarnings();

		if (errorAlreadyCaught != null) {
			errorAlreadyCaught.set(false);
		}

	}

	@Override
	public String getNextInputMessage() throws RioDBPluginException {

		String message;

		if (extremeMode) {
			message = streamArrayQueue.poll();
			while (message == null) {
				// wait and try again until not null
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					;
				}
				message = streamArrayQueue.poll();
			}
		} else {
			message = streamBlockingQueue.poll();
			if (message == null) {
				try {
					message = streamBlockingQueue.take();
				} catch (InterruptedException e) {
					logDebug("Queue interrupted.");
					return null;
				}
			}
		}

		if (message != null && message.length() > 0) {

			return message;

		}
		return null;
	}

	@Override
	public int getQueueSize() {
		if (extremeMode) {
			return streamBlockingQueue.size();
		}
		return streamArrayQueue.size();
	}

	@Override
	public void init(String inputParams) throws RioDBPluginException {

		logDebug("Initializing for INPUT with paramters (" + inputParams + ")");

		// get url parameter
		String url = PluginUtils.getParameter(inputParams, "url");
		if (url != null && url.contains(":")) {
			kafkaUrl = url;
		} else {
			throw new RioDBPluginException(
					this.getType() + "Requires 'url' parameter like 'mykafkaserver.myabcbiz.com:9092'.");
		}

		// get topic parameter
		String topic = PluginUtils.getParameter(inputParams, "topic");
		if (topic != null) {
			kafkaTopic = topic;
		} else {
			throw new RioDBPluginException(this.getType() + "Requires 'topic' parameter like 'mytopic'.");
		}

		// get topic parameter
		String group = PluginUtils.getParameter(inputParams, "group");
		if (group != null) {
			kafkaGroup = group;
		} else {
			throw new RioDBPluginException(this.getType() + "Requires 'group' parameter like 'my-group'.");
		}

		int maxCapacity = DEFAULT_MAX_CAPACITY;
		// get optional queue capacity
		String newMaxCapacity = PluginUtils.getParameter(inputParams, "queue_capacity");
		if (newMaxCapacity != null) {
			if (PluginUtils.isPositiveInteger(newMaxCapacity)) {
				maxCapacity = Integer.valueOf(newMaxCapacity);
			} else {
				pluginStatus.setFatal();
				throw new RioDBPluginException(
						this.getType() + "Requires positive intenger for 'queue_capacity' parameter.");
			}

		}

		// get character-set. Default is UTF-8
		String charSetParam = PluginUtils.getParameter(inputParams, "char_set");
		if (charSetParam != null) {
			if (charSetParam.equals("UTF_8")) {
				charSet = StandardCharsets.UTF_8;
			} else if (charSetParam.equals("US_ASCII")) {
				charSet = StandardCharsets.US_ASCII;
			} else if (charSetParam.equals("ISO_8859_1")) {
				charSet = StandardCharsets.ISO_8859_1;
			} else if (charSetParam.equals("UTF_16")) {
				charSet = StandardCharsets.UTF_16;
			} else if (charSetParam.equals("UTF_16BE")) {
				charSet = StandardCharsets.UTF_16BE;
			} else if (charSetParam.equals("UTF_16LE")) {
				charSet = StandardCharsets.UTF_16LE;
			} else {
				throw new RioDBPluginException("Unknown character set '" + charSetParam + "'");
			}
		} else {
			charSet = StandardCharsets.UTF_8;
		}

		String mode = PluginUtils.getParameter(inputParams, "mode");
		if (mode != null && mode.equals("extreme")) {
			extremeMode = true;
			streamArrayQueue = new SpscChunkedArrayQueue<String>(DEFAULT_QUEUE_INIT_CAPACITY, maxCapacity);
		} else {
			streamBlockingQueue = new LinkedBlockingQueue<String>(maxCapacity);
		}

		pluginStatus.setFatal();
		interrupt = new AtomicBoolean(true);
		errorAlreadyCaught.set(false);

		logDebug("Initialized.");

	}

	@Override
	public void run() {

		interrupt.set(false);
		logDebug("client process started.");

		// Creating consumer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroup);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// creating consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		// Subscribing
		consumer.subscribe(Arrays.asList(kafkaTopic));

		pluginStatus.setOk();

		// polling
		while (!interrupt.get()) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			if (records != null) {
				for (ConsumerRecord<String, String> record : records) {
					if (record.value() != null) {
						if (extremeMode) {
							streamArrayQueue.offer(record.value());
						} else {
							streamBlockingQueue.offer(record.value());
						}
					}
				}
			}
		}

		consumer.close();

		pluginStatus.setStopped();
		logDebug("client process terminated.");

	}

	@Override
	public void start() throws RioDBPluginException {
		interrupt.set(false);
		kafkaClientThread = new Thread(this);
		kafkaClientThread.setName("INPUT_KAFKA_THREAD");
		kafkaClientThread.start();
	}

	@Override
	public void stop() {
		logDebug("interrupting client.");
		interrupt.set(true);
	}

}
