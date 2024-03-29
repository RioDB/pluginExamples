/*
		TCP  (www.riodb.org)

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
   TCP is a RioDB plugin the listens as a socket server.
   It receives lines of text via TCP connection
   and stores them in a queue. 
   RioDB stream process can poll events from the queue. 
   
   TCP is single-threaded. Inserting data into RioDB is 
   so fast that it didn't make sense to spin multiple
   threads for accepting connections.  
   
   In the event that clients are uploading large files, 
   we prefer to ingest the files sequentially. So
   TCP is single-threaded. At least for now.  
  
 */
package org.riodb.plugin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jctools.queues.SpscChunkedArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpInput {
	// default queue limits
	public static final int DEFAULT_QUEUE_INIT_CAPACITY = 244; // 10000;
	public static final int DEFAULT_MAX_CAPACITY = 1000000;
	public static final int DEFAULT_CONN_BACKLOG = 1000;

	private Logger logger = LoggerFactory.getLogger(TCP.PLUGIN_NAME);
	private String logPrefix = "org.riodb.plugin."+ TCP.PLUGIN_NAME + ".TcpInput - ";

	private int portNumber;
	private int status = 0; // 0 idle; 1 started; 2 warning; 3 fatal

	// local copy of Stream mapping of which fields are number.
	private boolean numericFlags[];
	private int numberFieldCount;
	private int stringFieldCount;
	private int totalFieldCount;
	private int fieldMap[];

	private String fieldDelimiter = "\t";
	private DateTimeFormatter timestampFormat = null;
	private int timestampFieldId = -1;
	private boolean timestampMillis = false;

	// running in mode EFFICIENT vs EXTREME
	private boolean extremeMode = false;
	
	// An Inbox queue based on arrayQueue for extreme loads
	private SpscChunkedArrayQueue<String> streamArrayQueue;

	// An index queue based on blockingQueue for efficient processing
	private LinkedBlockingQueue<String> streamBlockingQueue;

	private ServerSocket serverSocket;
	private Socket clientSocket;

	// process flags.
	private AtomicBoolean interrupt;
	private boolean errorAlreadyCaught = false;

	private static final boolean isNumber(String s) {
		if (s == null)
			return false;
		try {
			Float.valueOf(s);
			return true;
		} catch (NumberFormatException nfe) {
			return false;
		}

	}

	public RioDBStreamMessage getNextInputMessage() throws RioDBPluginException {

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
			if(message == null) {
				try {
					message = streamBlockingQueue.take();
				} catch (InterruptedException e) {
					logger.debug(logPrefix +"Queue interrupted.");
					return null;
				}
			}
		}

		if (message != null && message.length() > 0) {
			
			
			// create new event.
			RioDBStreamMessage event = new RioDBStreamMessage(numberFieldCount, stringFieldCount);

			// split message fields by delimiter
			String fields[] = message.split(fieldDelimiter);

			if (fields.length >= totalFieldCount) {

				try {

					// iterate through message fields
					for (int i = 0; i < totalFieldCount; i++) {

						// if this field is the timestamp field
						if (i == timestampFieldId) {
							// if there is a format defined, then timestamp parse timestamp String into unix
							// epoch number.
							if (timestampFormat != null) {
								try {
									ZonedDateTime zdt = ZonedDateTime.parse(fields[i], timestampFormat);
									double epoch = zdt.toInstant().toEpochMilli() / 1000;
									event.set(fieldMap[i], epoch);
								} catch (java.time.format.DateTimeParseException e) {
									status = 2;
									if (!errorAlreadyCaught) {
										logger.warn(logPrefix + fields[i] +"' could not be parsed as '"+ timestampFormat.toString() +"'");
										errorAlreadyCaught = true;
									}
									return null;
								}

							} else if (timestampMillis){
								// can raise NumberFormatexception
								double d = (long)(Double.valueOf(fields[i])/1000);
								event.set(fieldMap[i], d);
							}
							else {
								// can raise NumberFormatexception
								event.set(fieldMap[i], Double.valueOf(fields[i]));
							}
						} else if (numericFlags[i]) {
							// can raise NumberFormatexception
							event.set(fieldMap[i], Double.valueOf(fields[i]));
						} else {
							event.set(fieldMap[i], fields[i]);
						}
					}
					return event;

				} catch (NumberFormatException nfe) {
					status = 2;
					if (!errorAlreadyCaught) {
						logger.warn(logPrefix +"Received INVALID NUMBER [" + message + "]");
						errorAlreadyCaught = true;
					}
					return null;
				}

			} else {
				status = 2;
				if (!errorAlreadyCaught) {
					logger.warn(logPrefix +"Received fewer values than expected. [" + message + "]");
					errorAlreadyCaught = true;
				}
			}

			
			
		}
		return null;
	}

	public int getQueueSize() {
		if(extremeMode) {
			return streamBlockingQueue.size();
		}
		return streamArrayQueue.size(); 
	}

	public void init(String datasourceParams, RioDBStreamMessageDef def) throws RioDBPluginException {

		logger.debug(logPrefix +"Initializing for INPUT with paramters (" + datasourceParams + ")");

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
		
		if (def.getTimestampNumericFieldId() >= 0) {
			this.timestampFieldId = def.getTimestampNumericFieldId();
			logger.trace(logPrefix +"TimestampNumericFieldId is " + timestampFieldId);
		}
		
		if(def.getTimestampFormat() != null) {
			this.timestampFormat = DateTimeFormatter.ofPattern(def.getTimestampFormat());
			logger.trace(logPrefix +"TimestampFormat is " + timestampFormat);
		}

		if (def.getTimestampMillis()) {
			timestampMillis = true;
		}

		String params[] = datasourceParams.split(" ");
		if (params.length < 2)
			throw new RioDBPluginException(logPrefix +"Requires a numeric 'port' parameter.");

		// get port parameter
		boolean portNumberSet = false;
		String port = getParameter(params, "port");
		if (port != null) {
			if (isNumber(port)) {
				portNumber = Integer.valueOf(port);
				portNumberSet = true;
			} else {
				status = 3;
				throw new RioDBPluginException(logPrefix +"Requires a numeric 'port' parameter.");
			}
		}
		if (!portNumberSet) {
			throw new RioDBPluginException(logPrefix +"Requires a numeric 'port' parameter.");
		}
		
		// get delimiter parameter
		String delimiter = getParameter(params, "delimiter");
		if (delimiter != null ) {
			if (delimiter.length() == 3) {
				fieldDelimiter = String.valueOf(delimiter.charAt(1));
			} else {
				status = 3;
				throw new RioDBPluginException(logPrefix +"a delimiter should be declared in single quotes, like ','");
			}
		}
		
		int maxCapacity = DEFAULT_MAX_CAPACITY;
		// get optional queue capacity
		String newMaxCapacity = getParameter(params, "queue_capacity");
		if (newMaxCapacity != null) {
			if (isNumber(newMaxCapacity) && Integer.valueOf(newMaxCapacity) > 0) {
				maxCapacity = Integer.valueOf(newMaxCapacity);
			} else {
				status = 3;
				throw new RioDBPluginException(
						logPrefix +"Requires positive intenger for 'queue_capacity' parameter.");
			}

		}

		String mode = getParameter(params, "mode");
		if (mode != null && mode.equals("extreme")) {
			extremeMode = true;
			streamArrayQueue = new SpscChunkedArrayQueue<String>(DEFAULT_QUEUE_INIT_CAPACITY, maxCapacity);
		} else {
			streamBlockingQueue = new LinkedBlockingQueue<String>(maxCapacity);
		}

		status = 0;
		interrupt = new AtomicBoolean(true);
		
		logger.debug(logPrefix +"Initialized.");

	}

	// start receiving data. 
	public void start() throws RioDBPluginException {
		interrupt.set(false);
		status = 1;
		logger.debug(logPrefix +"Listener process started.");
		
		try {

			serverSocket = new ServerSocket(portNumber, DEFAULT_CONN_BACKLOG);

			// loop for accepting incoming connections.
			while ((clientSocket = serverSocket.accept()) != null && !interrupt.get()) {
				clientSocket.setSoTimeout(1000);
				try {

					OutputStream output = clientSocket.getOutputStream();
					PrintWriter out = new PrintWriter(output, true);
					InputStreamReader streamReader = new InputStreamReader(clientSocket.getInputStream());
					BufferedReader in = new BufferedReader(streamReader);

					String received;

					// loops for processing lines of data.
					int counter = 0;
					while ((received = in.readLine()) != null) {
						if (received.equals(";")) {
							break;
						}

						if(extremeMode) {
							streamArrayQueue.offer(received);
						} else {
							streamBlockingQueue.offer(received);
						}
						counter++;
					}
					out.print("{\"received\":" + counter + "}");
					out.flush();
					in.close();
					out.close();
					clientSocket.close();
				} catch (IOException e) {
					if(!errorAlreadyCaught) {
						logger.warn(logPrefix +"IOException. Connection reset.");
						status = 2;
					}
				}
			}
			// }
			status = 0;
			if (!serverSocket.isClosed()) {
				logger.debug(logPrefix +"Closing socket server.");
				serverSocket.close();
			}

		} catch (IOException e) {
			if (interrupt.get()) {
				logger.info(logPrefix +"Listener process interrupted.");
				status = 0;
			} else {
				status = 3;
				logger.info(logPrefix +"Listener process interrupted UNEXPECTEDLY.");
				logger.debug(e.getMessage().replace('\n', ';').replace('\r', ';'));
			}
		}

		logger.debug(logPrefix +"Listener process terminated.");
	}

	public RioDBPluginStatus status() {
		return new RioDBPluginStatus(status);
	}

	public void stop() {
		logger.debug("Closing TCP socket.");
		interrupt.set(true);;
		try {
			if (!serverSocket.isClosed()) {
				logger.debug(logPrefix +"Closing TCP server socket");
				serverSocket.close();
			}
		} catch (IOException e) {
			logger.error(logPrefix +"Closing TCP sockets: " + e.getMessage().replace("\n", " ").replace("\r", " "));
		}
		logger.debug(logPrefix +"Interrupting TCP thread.");
		status = 0;
	}

	private String getParameter(String params[], String key) {
		for (int i = 0; i < params.length; i++) {
			if (key.equals(params[i].toLowerCase()) && i < params.length - 1) {
				return params[i + 1];
			}
		}
		return null;
	}

}
