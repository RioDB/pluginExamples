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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

@SuppressWarnings("restriction")
public class HttpInput {

	// a queue will be initialized with this initial capacity:
	public static final int QUEUE_INIT_CAPACITY = 244;
	// a queue has a max capacity, at which point new messages start getting
	// ignored.
	public static final int MAX_CAPACITY = 1000000;
	// access to logger
	private Logger logger = LoggerFactory.getLogger("RIODB");
	// if a bad message arrives (like invalid Number), we log the issue only once
	private boolean errorAlreadyCaught = false;
	// an instance of java HttpServer
	private HttpServer httpServer = null;
	// an instance of HttpInputHandler
	private final HttpInputHandler httpInputHandler = new HttpInputHandler();
	// host address
	InetSocketAddress socketAddress;
	// default port number to listen on.
	private int portNumber = 8080;
	// default http handler backlog buffer.
	private int backlog = 0;
	// default listening URL path
	private String urlPath = "/";

	// the next 3 booleans drive the content type
	private boolean applicationJson = false;
	private boolean applicationXml = false;
	private boolean textPlain = false;

	// the status of this Input Plugin
	private int status = 0; // 0 idle; 1 started; 2 warning; 3 fatal

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

	SynchronousQueue<Boolean> interrupt;

	// An Inbox queue to receive stream raw data from TCP or UDP listeners
	private final LinkedBlockingQueue<String> streamPacketInbox = new LinkedBlockingQueue<String>(MAX_CAPACITY);

	// HTTP request handler
	class HttpInputHandler implements HttpHandler {

		/*
		 * handle
		 * 
		 * Processes incoming HTTP request data Adds message data into queue
		 * 
		 */

		@Override
		public void handle(final HttpExchange t) throws IOException {

			String response = "{\"status\":200,\"message\":\"Ok.\"}";

			if (t.getRequestMethod().equals("POST")) {
				String payload = inputStreamToString(t.getRequestBody());

				if (payload != null && payload.length() > 0) {

					streamPacketInbox.offer(payload);

				}

			} else {

				response = "{\"status\": 501, \"message\":\"Method not implemented. Only POST is supported.\"}";

			}

			t.sendResponseHeaders(200, response.getBytes().length);
			OutputStream os = t.getResponseBody();
			os.write(response.getBytes());
			os.close();
		}
	}

	/*
	 * getNextInputMessage
	 * 
	 * RioDB engine runs this procedure to poll messages from queue.
	 */
	public RioDBStreamMessage getNextInputMessage() throws RioDBPluginException {

		String payload = streamPacketInbox.poll();

		if (payload == null) {
			try {
				payload = streamPacketInbox.take();
			} catch (InterruptedException e) {
				return null;
			}
		}

		if (textPlain) {
			return handleTextMsg(payload);// handleTextMsg(payload);
		}
		if (applicationJson) {
			return handleJsonMsg(payload);
		}
		if (applicationXml) {
			return handleXmlMsg(payload);
		}

		return null;
	}

	// get queue size.
	public int getQueueSize() {
		return streamPacketInbox.size();
	}

	/*
	 * initInput
	 * 
	 * initializes this object for use as Stream Input
	 */
	public void initInput(String listenerParams, RioDBStreamMessageDef def) throws RioDBPluginException {

		logger.debug(HTTP.PLUGIN_NAME + " INPUT - Initializing with paramters (" + listenerParams + ")");

		// GET CONFIGURABLE PARAMETERS:

		String params[] = listenerParams.split(" ");

		// set port parameter
		String portParam = getParameter(params, "port");
		if (portParam != null && portParam.length() > 0) {
			portParam = portParam.replace("'", "");
			if (isNumber(portParam) && Integer.valueOf(portParam) >= 0) {
				portNumber = Integer.valueOf(portParam);
			} else {
				status = 3;
				throw new RioDBPluginException("PORT attribute must be a positive intenger.");
			}
		} else {
			logger.debug(HTTP.PLUGIN_NAME + " INPUT -  using default port 8080 since a port was not specified");
		}

		// restrict listener to address if provided
		String addressParam = getParameter(params, "host");
		if (addressParam != null && addressParam.length() > 0) {
			addressParam = addressParam.replace("'", "");
			try {
				InetAddress address = InetAddress.getByName(addressParam);
				socketAddress = new InetSocketAddress(address, portNumber);
			} catch (UnknownHostException e) {
				status = 3;
				throw new RioDBPluginException("Unknown Host: " + addressParam);
			}
		} else {
			// if no address provided, then no restriction:
			socketAddress = new InetSocketAddress(portNumber);
		}

		// set port parameter
		String backlogParam = getParameter(params, "backlog");
		if (backlogParam != null && backlogParam.length() > 0) {
			backlogParam = backlogParam.replace("'", "");
			if (isNumber(backlogParam) && Integer.valueOf(backlogParam) >= 0) {
				backlog = Integer.valueOf(backlogParam);
			} else {
				status = 3;
				throw new RioDBPluginException("BACKLOG attribute must be an integer greater than or equal to zero.");
			}
		}

		// set URL
		String pathParam = getParameter(params, "path");
		if (pathParam != null && pathParam.length() > 0) {
			pathParam = pathParam.replace("'", "");
			urlPath = pathParam;
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
						fieldDelimiterChar = delimiter.charAt(1);
						fieldDelimiter = String.valueOf(fieldDelimiterChar);
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

		if (applicationJson) {
			fieldNames = new HashMap<String, Integer>();
			for (int i = 0; i < numericFlags.length; i++) {
				fieldNames.put(def.getFieldName(i), i);
			}
		}

		if (def.getTimestampNumericFieldId() >= 0) {
			this.timestampFieldId = def.getTimestampNumericFieldId();
			logger.debug(HTTP.PLUGIN_NAME + " INPUT - timestampNumericFieldId is " + timestampFieldId);
		}

		if (def.getTimestampFormat() != null) {
			this.timestampFormat = DateTimeFormatter.ofPattern(def.getTimestampFormat());
			logger.debug(HTTP.PLUGIN_NAME + " INPUT - timestampFormat is " + timestampFormat);
		}

		if (def.getTimestampMillis()) {
			timestampMillis = true;
		}
		logger.debug(HTTP.PLUGIN_NAME + " INPUT - Initialized using " + portParam + ":" + urlPath
				+ ", and contentType = " + contentTypeParam);

		interrupt = new SynchronousQueue<Boolean>();
	}

	/*
	 * start()
	 * 
	 * starts this input stream (when RioDB engine is started)
	 * 
	 */
	public void start() throws RioDBPluginException {
		logger.debug(HTTP.PLUGIN_NAME + " INPUT - Starting on port " + portNumber);
		try {

			httpServer = HttpServer.create(socketAddress, backlog);
			httpServer.createContext(urlPath, httpInputHandler);
			httpServer.setExecutor(null); // creates a default executor
			logger.info(
					HTTP.PLUGIN_NAME + " INPUT - Starting HTTPServer on " + portNumber + " with backlog = " + backlog);
			httpServer.start();
			status = 1;

		} catch (IOException e) {
			logger.error(HTTP.PLUGIN_NAME + " INPUT - Error starting HTTP interface: " + e.getMessage());
			httpServer = null;
			status = 3;
		}

		logger.debug(HTTP.PLUGIN_NAME + " INPUT - " + portNumber + " started.");

		// Park the thread here until stop() is issued.
		try {
			interrupt.take();
		} catch (InterruptedException e) {
		}

	}

	/*
	 * get status of this plugin
	 * 
	 */
	public RioDBPluginStatus status() {
		return new RioDBPluginStatus(status);
	}

	/*
	 * stop()
	 * 
	 * stops this plugin when RioDB is put offline.
	 */
	public void stop() {
		logger.debug(HTTP.PLUGIN_NAME + " INPUT - " + portNumber + " stopping...");
		if (httpServer != null) {
			httpServer.stop(0);
			httpServer = null;
			logger.debug(HTTP.PLUGIN_NAME + " INPUT - Stopped HTTP interface");
		}
		try {
			interrupt.put(true);
		} catch (InterruptedException e) {
		}
		status = 0;
		logger.debug(HTTP.PLUGIN_NAME + " INPUT - " + portNumber + " terminated.");
	}

	/*
	 * inputStreamToString
	 * 
	 * converts input stream to a string
	 * 
	 */
	private String inputStreamToString(InputStream inputStream) {

		String response = "";
		StringBuilder textBuilder = new StringBuilder();
		Reader reader = new BufferedReader(
				new InputStreamReader(inputStream, Charset.forName(StandardCharsets.UTF_8.name())));
		int c = 0;
		try {
			while ((c = reader.read()) != -1) {
				textBuilder.append((char) c);
			}
			response = textBuilder.toString();
		} catch (IOException e) {
			status = 2;
			if (!errorAlreadyCaught) {
				logger.warn(HTTP.PLUGIN_NAME + " INPUT - IOException while converting InputStream to String");
				errorAlreadyCaught = true;
			}
		}

		return response;

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
						status = 2;
						if (!errorAlreadyCaught) {
							logger.warn(HTTP.PLUGIN_NAME + " INPUT - Received an invalid json.");
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
									status = 2;
									if (!errorAlreadyCaught) {
										logger.warn(HTTP.PLUGIN_NAME + " INPUT - Field '" + value
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
								status = 2;
								if (!errorAlreadyCaught) {
									logger.warn(HTTP.PLUGIN_NAME + " INPUT - Received an invalid number.");
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
				if (!errorAlreadyCaught) {
					logger.warn(HTTP.PLUGIN_NAME + " INPUT - Received an invalid json payload.");
					errorAlreadyCaught = true;
				}
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
									status = 2;
									if (!errorAlreadyCaught) {
										logger.warn(HTTP.PLUGIN_NAME + " INPUT - Field '" + fields[i]
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
					status = 2;
					if (!errorAlreadyCaught) {
						logger.warn(HTTP.PLUGIN_NAME + " INPUT - Received an invalid number.");
						errorAlreadyCaught = true;
					}
					return null;
				}

			} else {
				status = 2;
				if (!errorAlreadyCaught) {
					logger.warn(HTTP.PLUGIN_NAME + " INPUT - Received fewer values than expected.");
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

	/*
	 * function to test if a string is an Integer. It's repeated in HttpOutput
	 * because creating a separate classes creates trouble with some classloaders at
	 * runtime
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

}
