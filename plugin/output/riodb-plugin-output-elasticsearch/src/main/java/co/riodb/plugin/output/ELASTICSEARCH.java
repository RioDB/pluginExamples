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

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.BinaryData;
import co.elastic.clients.util.ContentType;
import co.riodb.plugin.resources.LoggingService;
import co.riodb.plugin.resources.PluginUtils;
import co.riodb.plugin.resources.RioDBOutputPlugin;
import co.riodb.plugin.resources.RioDBPluginException;


public class ELASTICSEARCH extends RioDBOutputPlugin {

    public ELASTICSEARCH(LoggingService loggingService) {
        super(loggingService);
    }

    // queue limits
    public static final int QUEUE_INIT_CAPACITY = 244; // 10000;
    public static final int MAX_CAPACITY = 1000000;

    // a local copy of the URL that requests will be sent to.
    private String host;

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
    private boolean errorAlreadyCaught = false;

    // Elasticsearch Client
    private ElasticsearchClient esClient;

    // index name:
    private String index = "riodb";

    // batch size
    private int batchSize = 100;


    // Method that child thread calls to dequeue messages from the queue
    private void dequeAndSend() {

        logDebug( "Worker Thread [" + Thread.currentThread().getName() + "] - started.");

        boolean receivedPoison = false;

        while (!interrupt.get()) {

            // the bulkrequest to elasticsearch has a batch_size.
            // So get as many documents as are ready in the queue to fill up the batch.
            // If the queue doesn't have enough, just go with what we got.

            ArrayList<String> documents = new ArrayList<String>();

            while (documents.size() < batchSize) {

                // a quick poll in case queue is not empty
                String document = outgoingBlockingQueue.poll();
                if (document == null) {
                    break;
                } else if (document.equals(POISON)) {
                    receivedPoison = true;
                    break;
                }
                documents.add(document);
            }

            // if received poison, exit
            if (receivedPoison) {
                logDebug(
                        "Worker Thread [" + Thread.currentThread().getName() + "] - received poison.");
                break;
            }
            // if the queue didn't have anything ready, wait for next
            else if (documents.size() == 0) {
                // take() and wait for something to be received.
                try {
                    String document = outgoingBlockingQueue.take();
                    documents.add(document);
                    if (document.equals(POISON)) {
                        receivedPoison = true;
                    }
                } catch (InterruptedException e) {
                    logDebug( "WorkerThread [" + Thread.currentThread().getName()
                            + "] -interrupted.");
                    break; // terminate loop
                }
            }
            // if received poison, exit
            if (receivedPoison) {
                logDebug(
                        "Worker Thread [" + Thread.currentThread().getName() + "] - received poison.");
                break;
            }

            if (documents.size() > 0) {

                BulkRequest br = makeBulkRequest(documents);

                sendBulkRequest(br);

            }

        } // end while loop

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

        host = "localhost";
        int port = 9200;
        String username = "";
        String password = "";

        // set destination URL
        String urlParam = PluginUtils.getParameter(outputParams, "url");
        if (urlParam != null && urlParam.length() > 0) {
            host = urlParam.trim().replace("'", "");
        } else {
            pluginStatus.setFatal();
            throw new RioDBPluginException(this.getType() + " output requires 'url' parameter, like: url 'localhost'");
        }

        // set port
        String portParam = PluginUtils.getParameter(outputParams, "port");
        if (portParam != null) {
            if (PluginUtils.isPositiveInteger(portParam)) {
                port = Integer.valueOf(portParam);
            } else {
                pluginStatus.setFatal();
                throw new RioDBPluginException(
                        this.getType() + " output requires positive intenger for 'port' parameter.");
            }
        }

        // set index
        String indexParam = PluginUtils.getParameter(outputParams, "index");
        if (indexParam != null && indexParam.length() > 0) {
            index = indexParam.trim().replace("'", "");
            ;
        }

        // set user
        String userParam = PluginUtils.getParameter(outputParams, "user");
        if (userParam != null && userParam.length() > 0) {
            userParam = userParam.trim().replace("'", "");
            username = userParam;
        } else {
            throw new RioDBPluginException(
                    this.getType() + " output requires user, like:  user 'kimchy'");
        }

        // set password
        String passParam = PluginUtils.getParameter(outputParams, "password");
        if (passParam != null && passParam.length() > 0) {
            passParam = passParam.trim().replace("'", "");
            password = passParam;
        } else {
            throw new RioDBPluginException(
                    this.getType() + " output requires password, like:  password 'sdf*&^'");
        }

        int maxCapacity = MAX_CAPACITY;
        // get optional queue capacity
        String capacityParam = PluginUtils.getParameter(outputParams, "queue_capacity");
        if (capacityParam != null) {
            if (PluginUtils.isPositiveInteger(capacityParam)) {
                maxCapacity = Integer.valueOf(capacityParam);
            } else {
                pluginStatus.setFatal();
                throw new RioDBPluginException(
                        this.getType() + " output requires positive intenger for 'max_capacity' parameter.");
            }
        }
        outgoingBlockingQueue = new LinkedBlockingQueue<String>(maxCapacity);

        // if user opted for extreme mode...
        String workersParam = PluginUtils.getParameter(outputParams, "workers");
        if (workersParam != null) {
            if (PluginUtils.isPositiveInteger(workersParam)) {
                workers = Integer.valueOf(workersParam);
            } else {
                pluginStatus.setFatal();
                throw new RioDBPluginException(
                        this.getType() + " output requires positive intenger for 'workers' parameter.");
            }
        }

        // if user opted for extreme mode...
        String batchSizeParam = PluginUtils.getParameter(outputParams, "batch_size");
        if (batchSizeParam != null) {
            if (PluginUtils.isPositiveInteger(batchSizeParam)) {
                workers = Integer.valueOf(batchSizeParam);
            } else {
                pluginStatus.setFatal();
                throw new RioDBPluginException(
                        this.getType() + " output requires positive intenger for 'batch_size' parameter.");
            }
        }

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(host, port))
                .setHttpClientConfigCallback(new HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder
                                .setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        // Create the transport with a Jackson mapper
        ElasticsearchTransport elasticSearchTransport = new RestClientTransport(builder.build(), new JacksonJsonpMapper());

        // And create the API client
        esClient = new ElasticsearchClient(elasticSearchTransport);

        interrupt = new AtomicBoolean();

        logDebug( "Initialized.");
    }

    /*
     * makeBulkRequest
     * 
     * function to build an elasticsearch bulk request payload
     * 
     */
    private BulkRequest makeBulkRequest(ArrayList<String> documents) {

        BulkRequest.Builder bulkRequest = new BulkRequest.Builder();

        // Iterate over documents and add them to bulk request
        for (String eachDocument : documents) {
            if (documents != null) {
                BinaryData data = BinaryData.of(eachDocument.getBytes(), ContentType.APPLICATION_JSON);
                bulkRequest.operations(op -> op
                        .index(idx -> idx
                                .index(index)
                                .document(data)));
            }
        }

        return bulkRequest.build();

    }

    /*
     * makeBulkRequest
     * 
     * function to build an elasticsearch bulk request payload
     * 
     */
    private void sendBulkRequest(BulkRequest bulk) {

        BulkResponse bulkResponse;
        try {
            bulkResponse = esClient.bulk(bulk);
            // Log errors, if any
            if (bulkResponse.errors()) {
                for (BulkResponseItem item : bulkResponse.items()) {
                    if (item.error() != null && !errorAlreadyCaught) {
                        errorAlreadyCaught = true;
                        pluginStatus.setWarning();
                        @SuppressWarnings("null")
                        String reasonStr = item.error().reason();
                        logDebug( "Response error: " + reasonStr);
                    }
                }
            }
        } catch (ElasticsearchException e) {
            if (!errorAlreadyCaught) {
                errorAlreadyCaught = true;
                pluginStatus.setWarning();
                logDebug( "ElasticsearchException: " + e.getMessage());
            }
        } catch (IOException e) {
            if (!errorAlreadyCaught) {
                errorAlreadyCaught = true;
                pluginStatus.setWarning();
                logDebug( "IOException: " + e.getMessage());
            }
        }
    }

    /*
     * sendOutput method called by parent HTTP class to send output request via HTTP
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
        errorAlreadyCaught = false;

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
