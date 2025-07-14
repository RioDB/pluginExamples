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
package co.riodb.plugin.output;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import co.riodb.plugin.resources.LoggingService;
import co.riodb.plugin.resources.PluginUtils;
import co.riodb.plugin.resources.RioDBOutputPlugin;
import co.riodb.plugin.resources.RioDBPluginException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class KAFKA extends RioDBOutputPlugin {

    public KAFKA(LoggingService loggingService) {
        super(loggingService);
    }


    // queue limits
    public static final int QUEUE_INIT_CAPACITY = 244; // 10000;
    public static final int MAX_CAPACITY = 1000000;

    // An Outgoing queue based on blockingQueue for efficient processing
    private LinkedBlockingQueue<String> outgoingBlockingQueue;

    // number of worker threads to send output in parallel
    private int workers = 1;
    private ExecutorService workerPool;

    // a poison pill to interrupt blocked take()
    private final String POISON = "#!#POISON#!#";

    // boolean variable for interrupting thread while() loop.
    private AtomicBoolean interrupt;

    // if an error occurs (like invalid number)
    // we only log it once.
    //private boolean errorAlreadyCaught = false;

    // kafka Client
    private String url;
    private String topic;

    // batch size
    private int batchSize = -1;
    private int lingerMs = -1;
    private int retries = -1;
    private String key = null;


    // Method that child thread calls to dequeue messages from the queue
    private void dequeAndSend() {

        logDebug( "Worker Thread [" + Thread.currentThread().getName() + "] - started.");

        //boolean receivedPoison = false;

        Producer<String, String> producer = createProducer();

        try {

            while (!interrupt.get()) {

                // a quick poll in case queue is not empty
                String document = outgoingBlockingQueue.poll();

                // if the queue didn't have anything ready (NULL), wait for next
                if (document == null) {
                    // take() and wait for something to be received.
                    try {
                        document = outgoingBlockingQueue.take();
                    } catch (InterruptedException e) {
                        logDebug( "WorkerThread [" + Thread.currentThread().getName()
                                + "] -interrupted.");
                        break; // terminate loop
                    }
                }

                if (document != null) {
                    if (document.equals(POISON)) {
                        logDebug( "Worker Thread [" + Thread.currentThread().getName()
                                + "] - received poison.");
                        break;
                    } else {

                        ProducerRecord<String, String> recordToSendToKafka;

                        if (key == null) {
                            recordToSendToKafka = new ProducerRecord<>(topic, document);
                        } else {
                            recordToSendToKafka = new ProducerRecord<>(topic, key, document);
                        }
                        // asynchronous send
                        producer.send(recordToSendToKafka);

                    }
                }

            } // end while loop

        } finally

        {
            producer.flush();
            producer.close();
        }

        logDebug( "Worker Thread [" + Thread.currentThread().getName() + "] -stopped.");

        pluginStatus.setStopped();

    }

    // get queue size
    @Override
    public int getQueueSize() {

        return outgoingBlockingQueue.size();

    }

    /*
     * 
     * initialize this input plugin
     */
    @Override
    public void init(String outputParams) throws RioDBPluginException {
        logDebug( "Initializing with paramters (" + outputParams + ")");

        // set destination URL
        String urlParam = PluginUtils.getParameter(outputParams, "url");
        if (urlParam != null && urlParam.contains(":")) {
            url = urlParam.trim().replace("'", "");
        } else {
            pluginStatus.setFatal();
            throw new RioDBPluginException(this.getType() + " output requires 'url' parameter, like 'myserverabc.com:9092'");
        }

        // set topic
        String topicParam = PluginUtils.getParameter(outputParams, "topic");
        if (topicParam != null) {
            topic = topicParam.trim().replace("'", "");
        } else {
            pluginStatus.setFatal();
            throw new RioDBPluginException(this.getType() + " output requires 'topic' parameter, like 'mytopic'");
        }

        int maxCapacity = MAX_CAPACITY;
        // get optional queue capacity
        String capacityParam = PluginUtils.getParameter(outputParams, "queue_capacity");
        if (capacityParam != null) {
            if (PluginUtils.isPositiveInteger(capacityParam)) {
                maxCapacity = Integer.valueOf(capacityParam);
            } else {
                pluginStatus.setFatal();
                throw new RioDBPluginException(this.getType() + " output requires positive intenger for 'max_capacity' parameter.");
            }
        }
        outgoingBlockingQueue = new LinkedBlockingQueue<String>(maxCapacity);

        // if user opted for extreme mode...
        String workersParam = PluginUtils.getParameter(outputParams, "workers");
        if (workersParam != null) {
            if (PluginUtils.isPositiveInteger(workersParam)) {
                workers = Integer.valueOf(workersParam);
            }
        }

        // kafka producer batch size
        String batchSizeParam = PluginUtils.getParameter(outputParams, "batch_size");
        if (batchSizeParam != null) {
            if (PluginUtils.isPositiveInteger(batchSizeParam)) {
                batchSize = Integer.valueOf(batchSizeParam);
            }
        }

        // kafka producer linger ms
        String lingerMsParam = PluginUtils.getParameter(outputParams, "linger_ms");
        if (lingerMsParam != null) {
            if (PluginUtils.isPositiveInteger(lingerMsParam)) {
                lingerMs = Integer.valueOf(lingerMsParam);
            }
        }

        // kafka producer retries
        String retriesParam = PluginUtils.getParameter(outputParams, "retries");
        if (retriesParam != null) {
            if (PluginUtils.isPositiveInteger(retriesParam)) {
                retries = Integer.valueOf(retriesParam);
            }
        }

        // kafka producer retries
        String keyParam = PluginUtils.getParameter(outputParams, "key");
        if (keyParam != null) {
            key = keyParam;
        }

        interrupt = new AtomicBoolean();

        logDebug( "Initialized.");
    }

    /*-  
     *   createProducer
     *   
     *   function to build a kafka producer
     * 
     */
    private Producer<String, String> createProducer() {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        if (batchSize > 0) {
            kafkaProps.put("batch.size", batchSize);
        }

        if (batchSize >= 0) {
            kafkaProps.put("linger.ms", lingerMs);
        }

        if (retries >= 0) {
            kafkaProps.put("linger.ms", retries);
        }

        return new KafkaProducer<String, String>(kafkaProps);
    }

    /*
     * sendOutput method
     * 
     */
    @Override
    public void sendOutput(String message) {

        outgoingBlockingQueue.offer(message);

    }

    /*
     * start() starts all worker threads
     * 
     */
    @Override
    public void start() throws RioDBPluginException {

        logDebug( "starting " + workers + " workers...");

        // remove old data or POISON from queue
        outgoingBlockingQueue.clear();

        interrupt.set(false);
        //errorAlreadyCaught = false;

        // setup worker pool
        workerPool = Executors.newFixedThreadPool(workers);

        // define the run task
        Runnable task = () -> {
            dequeAndSend();
        };

        // run 1 task for each worker
        for (int i = 0; i < workers; i++) {
            workerPool.execute(task);
        }

        pluginStatus.setOk();
        logDebug( "Started.");

    }



    /*
     * stops all worker threads
     * 
     */
    @Override
    public void stop() throws RioDBPluginException {
        logDebug( "Stopping...");
        interrupt.set(true);

        // offer POISON to the queue in case there are worker threads waiting for data.
        for (int i = 0; i < workers; i++) {
            outgoingBlockingQueue.offer(POISON);
        }
    }


}
