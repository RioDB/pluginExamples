/*
			HTTP   (www.riodb.org)

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
   	HTTP is a RioDBPlugin that can be used as INPUT or OUTPUT.
   
   	INPUT: 
   	HTTP receives messages using java HttpServer
   
	OUTPUT
	HTTP sends message to a specified destination
	using java HttpClient. 
	
	
	www.riodb.org
  
 */
package org.riodb.plugin;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HttpOutput {

	// default HTTP request connection timeout in seconds
	private static final int DEFAULT_TIMEOUT = 10;

	// queue limits
	public static final int QUEUE_INIT_CAPACITY = 244; // 10000;
	public static final int MAX_CAPACITY = 1000000;

	// logger
	private Logger logger = LogManager.getLogger(HTTP.class.getName());

	// if an error occurs with request, we only log it once.
	private boolean errorAlreadyCaught = false;

	// status of this output plugin
	private int status = 0; // 0 idle; 1 started; 2 warning; 3 fatal

	// a copy of the strea message definition - field headers
	private String columnHeaders[];

	// a local coppy of the URL that requests will be sent to.
	private String url;
	// if URL contains subsitution variables like ${field_name}
	private boolean dynamicUrl = false;

	// booleands to direct the HTTP content_type processing
	private boolean applicationJson = false;
	private boolean applicationXml = false;
	private boolean textPlain = false;

	// User may elect to create content using a predefined template file:
	private String templateFile = null;

	// in case of XML and JSON, the parent key that data is nested under
	private String documentParent = null;

	// in case of text/plain, the field delimiter
	private String fieldDelimiter = ",";

	// booleans to direct the HTTP METHOD
	private boolean usePostMethod = false;
	private boolean useGetMethod = false;
	private boolean usePutMethod = false;

	// request connection timeout
	private int timeout = DEFAULT_TIMEOUT;
	// a BuilderObject to store initialized parameters, like proxy
	java.net.http.HttpClient.Builder httpClientBuilder;

	// An Outgoing queue based on blockingQueue for efficient processing
	private LinkedBlockingQueue<String[]> streamBlockingQueue;

	// number of worker threads to send output in parallel
	private int workers = 1;
	private ExecutorService workerPool;

	// boolean variable for interrupting thread while() loop.
	private boolean interrupt;

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

	// Method that child thread calls to dequeue messages from the queue
	public void dequeAndSend() {

		httpClientBuilder.connectTimeout(Duration.ofSeconds(timeout));

		// httpClientBuilder.authenticator(Authenticator.getDefault());

		HttpClient httpClient = httpClientBuilder.build();

		logger.debug(HTTP.PLUGIN_NAME + " Output Worker Thread [" + Thread.currentThread().getName() + "] - started.");

		while (!interrupt) {

			String columns[];

			// a quick poll in case queue is not empty
			columns = streamBlockingQueue.poll();

			// if queue was empty
			if (columns == null || columns.length == 0) {

				/// then we take() and wait for something to be received.
				try {
					columns = streamBlockingQueue.take();
				} catch (InterruptedException e) {
					logger.debug(HTTP.PLUGIN_NAME + " Output WorkerThread [" + Thread.currentThread().getName()
							+ "] -interrupted.");
					break;
				}
			}

			if (columns != null && columns.length > 0) {
				HttpRequest request = makeHTTPrequest(columns);

				HttpResponse<String> response;

				try {
					response = httpClient.send(request, BodyHandlers.ofString());
					int statusCode = response.statusCode();
					if (statusCode != 200) {
						if (!errorAlreadyCaught) {
							errorAlreadyCaught = true;
							status = 2;
							logger.warn(HTTP.PLUGIN_NAME + " Output - received HTTP response code " + statusCode);
						}
					}
				} catch (IOException e) {
					if (!errorAlreadyCaught) {
						errorAlreadyCaught = true;
						status = 2;
						logger.warn(HTTP.PLUGIN_NAME + " Output IOException: " + e.getMessage().replace("\n", " "));
					}
				} catch (InterruptedException e) {
					if (!errorAlreadyCaught) {
						errorAlreadyCaught = true;
						status = 2;
						logger.warn(HTTP.PLUGIN_NAME + " Output InterruptedException, possibly request timed out: "
								+ e.getMessage().replace("\n", " "));
					}
				}
			}

		} // end while loop

		logger.debug(HTTP.PLUGIN_NAME + " Output Worker Thread [" + Thread.currentThread().getName() + "] -stopped.");

		status = 0;

	}

	// get queue size
	public int getQueueSize() {

		return streamBlockingQueue.size();

	}

	/*
	 * 
	 * initialize this input plugin
	 */
	public void initOutput(String outputParams, String columnHeaders[]) throws RioDBPluginException {
		logger.debug("initializing " + HTTP.PLUGIN_NAME + " plugin for INPUT with paramters (" + outputParams + ")");

		this.columnHeaders = columnHeaders;

		// GET CONFIGURABLE PARAMETERS:

		String params[] = outputParams.split(" ");

		String proxyAddress;

		// set proxy
		// default is system-wide proxy settings
		ProxySelector proxySelector = ProxySelector.getDefault();
		String proxyParam = getParameter(params, "proxy");
		if (proxyParam != null && proxyParam.length() > 0) {
			proxyParam = proxyParam.replace("'", "");
			if (proxyParam.equals("none")) {
				proxySelector = null;
				logger.debug(HTTP.PLUGIN_NAME + " output - forcing no_proxy.");
			} else {
				proxyAddress = proxyParam.replace("'", "");
				String urlParts[] = proxyAddress.split(":");
				if (urlParts.length == 2 && isNumber(urlParts[1])) {
					proxySelector = ProxySelector.of(new InetSocketAddress(urlParts[0], Integer.valueOf(urlParts[1])));
					logger.debug(HTTP.PLUGIN_NAME + " output - using proxy '" + proxyAddress + "' ");
				} else {
					status = 3;
					throw new RioDBPluginException(HTTP.PLUGIN_NAME
							+ " Output - proxy parameter needs port, like: 'host.domain.com:8080' , or 'none' to force no proxy. ");
				}
			}
		}

		// set destination URL
		String urlParam = getParameter(params, "url");
		if (urlParam != null && urlParam.length() > 0) {
			url = urlParam.replace("'", "");
			if (!url.startsWith("http")) {
				status = 3;
				throw new RioDBPluginException(
						HTTP.PLUGIN_NAME + " output - 'url' parameter must start with 'http://' or 'https://' ");
			}
			if (url.contains("${")) {
				dynamicUrl = true;
				logger.debug(HTTP.PLUGIN_NAME + " output - using dynamic URL.");
			}
		} else {
			status = 3;
			throw new RioDBPluginException(HTTP.PLUGIN_NAME + " output requires 'url' parameter.");
		}

		// set document parent key (for json or xml)
		String parentParam = getParameter(params, "parent_key");
		if (parentParam != null && parentParam.length() > 0) {
			documentParent = parentParam.replace("'", "");
			logger.debug(HTTP.PLUGIN_NAME + " output - using document parent key '" + documentParent + "' ");
		}

		// set document parent key (for json or xml)
		String templateFileParam = getParameter(params, "template_file");
		if (templateFileParam != null && templateFileParam.length() > 0) {
			templateFileParam = templateFileParam.replace("'", "");
			setTemplateFile(templateFileParam);
			logger.debug(HTTP.PLUGIN_NAME + " output - using template file '" + templateFileParam + "' ");
		}

		// set connection timeout
		String timeoutParam = getParameter(params, "timeout");
		if (timeoutParam != null) {
			if (isNumber(timeoutParam) && Integer.valueOf(timeoutParam) > 0) {
				timeout = Integer.valueOf(timeoutParam);
				logger.debug(HTTP.PLUGIN_NAME + " output - using connection timeout " + timeoutParam + " ");
			} else {
				status = 3;
				throw new RioDBPluginException(HTTP.PLUGIN_NAME
						+ " output 'timeout' parameter should be a positive intenger representing seconds. Default is: "
						+ DEFAULT_TIMEOUT);
			}
		}

		// set method
		String methodParam = getParameter(params, "method");
		if (methodParam != null && methodParam.length() > 0) {
			methodParam = methodParam.toLowerCase().replace("'", "");
			if (methodParam.equals("get")) {
				useGetMethod = true;
				logger.debug(HTTP.PLUGIN_NAME + " output - using method 'GET' ");
			} else if (methodParam.equals("put")) {
				usePutMethod = true;
				logger.debug(HTTP.PLUGIN_NAME + " output - using method 'PUT' ");
			}
			if (methodParam.equals("post")) {
				usePostMethod = true;
				logger.debug(HTTP.PLUGIN_NAME + " output - using method 'POST' ");
			} else {
				status = 3;
				throw new RioDBPluginException(HTTP.PLUGIN_NAME + " output only supports GET, POST and PUT methods.");
			}
		} else {
			usePostMethod = true;
			logger.debug(HTTP.PLUGIN_NAME + " output - using method 'POST' by default.");
		}

		// set content_type
		String contentTypeParam = getParameter(params, "content_type");
		if (contentTypeParam != null && contentTypeParam.length() > 0) {
			contentTypeParam = contentTypeParam.replace("'", "");
			if (contentTypeParam.equals("application/json")) {
				applicationJson = true;
			} else if (contentTypeParam.equals("text/plain")) {
				textPlain = true;
				// get delimiter parameter, for text contentType
				String delimiter = getParameter(params, "delimiter");
				if (delimiter != null) {
					if (delimiter.length() == 3) {
						fieldDelimiter = String.valueOf(delimiter.charAt(1));
					} else {
						status = 3;
						throw new RioDBPluginException(
								"Syntax error. Try setting the delimiter in quotes:   DELIMITER ',' or '	' ");
					}
				}
			} else {
				status = 3;
				throw new RioDBPluginException(
						"At this time, the only options for CONTENT_TYPE are 'application/json' and 'text/plain'.");
			}
		} else {
			textPlain = true;
			contentTypeParam = "text/plain";
		}

		int maxCapacity = MAX_CAPACITY;
		// get optional queue capacity
		String newMaxCapacity = getParameter(params, "queue_capacity");
		if (newMaxCapacity != null) {
			if (isNumber(newMaxCapacity) && Integer.valueOf(newMaxCapacity) > 0) {
				maxCapacity = Integer.valueOf(newMaxCapacity);
			} else {
				status = 3;
				throw new RioDBPluginException(
						HTTP.PLUGIN_NAME + " output requires positive intenger for 'max_capacity' parameter.");
			}

		}

		// if user opted for extreme mode...
		String newWorkers = getParameter(params, "workers");
		if (newWorkers != null) {
			if (isNumber(newWorkers) && Integer.valueOf(newWorkers) > 0) {
				workers = Integer.valueOf(newWorkers);
			} else {
				status = 3;
				throw new RioDBPluginException(
						HTTP.PLUGIN_NAME + " output requires positive intenger for 'workers' parameter.");
			}
		}

		streamBlockingQueue = new LinkedBlockingQueue<String[]>(maxCapacity);

		httpClientBuilder = HttpClient.newBuilder()
				// .version(Version.HTTP_1_1)
				.followRedirects(Redirect.NORMAL);

		if (proxySelector != null) {
			httpClientBuilder.proxy(proxySelector);
		}

		logger.debug("Initialized " + HTTP.PLUGIN_NAME + " plugin for INPUT.");
	}

	/*
	 * function to test if a string is an Integer. It's repeated in HttpInput
	 * because creating a separate classes creates trouble with some classloaders at
	 * runtime. *
	 */
	private boolean isNumber(String s) {
		if (s == null)
			return false;
		try {
			Integer.parseInt(s);
			return true;
		} catch (NumberFormatException nfe) {
			return false;
		}
	}

	/*
	 * If using a destination URL with substitution variables like
	 * http://domain.com/${fieldName1}/
	 */
	private String makeURL(String columns[]) {
		String tempUrl = url;
		for (int i = 0; i < columnHeaders.length; i++) {
			if (tempUrl.contains("${" + columnHeaders[i] + "}")) {
				tempUrl = tempUrl.replace(("${" + columnHeaders[i] + "}"), columns[i]);
			}
		}
		return tempUrl;
	}

	/*
	 * Function to convert the Query Columns into a application/json payload
	 * 
	 */
	private String payloadAsJson(String columns[]) {
		StringBuilder payload = new StringBuilder("{");

		if (documentParent != null) {
			payload.append("\n \"").append(documentParent).append("\": {");
		}

		for (int i = 0; i < columns.length; i++) {
			String value = columns[i];
			if (value.contains("\"")) {
				value.replace("\"", "\\\"");
			}
			if (value.contains("\n")) {
				value.replace("\n", "\\n");
			}
			payload.append("\n  \"").append(columnHeaders[i]).append("\": \"").append(value).append("\"");
			if (i < columns.length - 1) {
				payload.append(",");
			}
		}

		if (documentParent != null) {
			payload.append("\n }");
		}

		payload.append("\n}");
		return payload.toString();
	}

	/*
	 * Function to convert the Query Columns into a text/plain payload
	 * 
	 */
	private String payloadAsText(String columns[]) {
		StringBuilder payload = new StringBuilder();
		for (int i = 0; i < columns.length; i++) {
			if (i > 0) {
				payload.append(fieldDelimiter);
			}
			if (columns[i].contains(fieldDelimiter)) {
				payload.append("\"").append(columns[i]).append("\"");
			} else {
				payload.append(columns[i]);
			}
		}
		return payload.toString();
	}

	/*
	 * Function to convert the Query Columns into a application/xml payload
	 * 
	 */
	private String payloadAsXml(String columns[]) {

		StringBuilder payload = new StringBuilder("<");
		payload.append(documentParent).append(">");

		for (int i = 0; i < columns.length; i++) {
			payload.append("\n  <" + columnHeaders[i] + ">").append(columns[i]).append("<\\" + columnHeaders[i] + ">");
		}

		payload.append("<\\" + documentParent + ">");
		return payload.toString();
	}

	/*
	 * Function to convert the Query Columns into a payload using a template file
	 * with variable substitution
	 * 
	 * if using template file (optional), this runs for every send operation
	 * 
	 */
	private String payloadFromTemplateFile(String columns[]) {
		String payload = templateFile;
		for (int i = 0; i < columnHeaders.length; i++) {
			if (payload.contains("${" + columnHeaders[i] + "}")) {
				payload = payload.replace(("${" + columnHeaders[i] + "}"), columns[i]);
			}
		}
		return payload;
	}

	/*
	 * sendOutput method called by parent HTTP class to send output request via HTTP
	 * 
	 */
	public void sendOutput(String columns[]) {

		streamBlockingQueue.offer(columns);

	}

	/*
	 * makeHTTPrequest
	 * 
	 * function to build an HttpRequest payload based on query columns
	 * 
	 */
	private HttpRequest makeHTTPrequest(String columns[]) {
		java.net.http.HttpRequest.Builder httpRequestBuilder = HttpRequest.newBuilder();

		if (dynamicUrl) {
			httpRequestBuilder.uri(URI.create(makeURL(columns)));
		} else {
			httpRequestBuilder.uri(URI.create(url));
		}
		httpRequestBuilder.header("User-agent", "RioDB-" + HTTP.PLUGIN_NAME + "-Output/" + HTTP.VERSION);
		httpRequestBuilder.timeout(Duration.ofSeconds(timeout));

		String payload = null;

		if (templateFile != null) {
			payload = payloadFromTemplateFile(columns);
		}

		// SET HTTP CONTENT-TYPE
		if (textPlain) {
			httpRequestBuilder.header("Content-Type", "text/plain");
			if (payload == null) {
				payload = payloadAsText(columns);
			}
		} else if (applicationJson) {
			httpRequestBuilder.header("Content-Type", "application/json");
			if (payload == null) {
				payload = payloadAsJson(columns);
			}
		} else if (applicationXml) {
			httpRequestBuilder.header("Content-Type", "application/xml");
			if (payload == null) {
				payload = payloadAsXml(columns);
			}
		}

		// SET HTTP METHOD
		if (useGetMethod) {
			httpRequestBuilder.GET();
		} else if (usePostMethod) {
			httpRequestBuilder.POST(BodyPublishers.ofString(payload));
		} else if (usePutMethod) {
			httpRequestBuilder.PUT(BodyPublishers.ofString(payload));
		}

		return httpRequestBuilder.build();

	}

	/*
	 * Function to obtain a template file (optional) this only runs once during
	 * initialization
	 */
	private void setTemplateFile(String fileName) throws RioDBPluginException {

		logger.debug(HTTP.PLUGIN_NAME + " output - loading template file: " + fileName);

		try {

			Path filePath = Path.of(fileName);
			templateFile = Files.readString(filePath);

		} catch (FileNotFoundException e) {
			logger.error(HTTP.PLUGIN_NAME + " output - template file not found: " + fileName);
			throw new RioDBPluginException(HTTP.PLUGIN_NAME + " output - template file not found: " + fileName);
		} catch (IOException e) {
			logger.error(HTTP.PLUGIN_NAME + " output - loading template file: " + e.getMessage().replace("\n", "\\n"));
			throw new RioDBPluginException(
					HTTP.PLUGIN_NAME + " output - loading template file: " + e.getMessage().replace("\n", "\\n"));
		}

	}

	/*
	 * start() starts all worker threads
	 * 
	 */
	public void start() throws RioDBPluginException {

		errorAlreadyCaught = false;
		logger.debug("HTTP Output Worker starting...");
		interrupt = false;
		workerPool = Executors.newFixedThreadPool(workers);
		Runnable task = () -> {
			dequeAndSend();
		};
		for (int i = 0; i < workers; i++) {
			workerPool.execute(task);
		}

		status = 1;
	}

	/*
	 * get plugin status
	 * 
	 */
	public RioDBPluginStatus status() {
		return new RioDBPluginStatus(status);
	}

	/*
	 * stops all worker threads
	 * 
	 */
	public void stop() throws RioDBPluginException {
		logger.debug("TCP Output Worker stopping...");
		interrupt = true;
		try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
		}
		workerPool.shutdown();
		status = 0;
	}
}
