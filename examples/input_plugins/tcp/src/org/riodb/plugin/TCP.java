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
 *  TCP is a RioDB plugin the listens as a socket server.
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jctools.queues.SpscChunkedArrayQueue;

public class TCP implements RioDBDataSource, Runnable {
	public static final int QUEUE_INIT_CAPACITY = 244; // 10000;
	public static final int MAX_CAPACITY = 1000000;
	public static final int DEFAULT_CONN_BACKLOG = 1000;

	private Logger logger = LogManager.getLogger(TCP.class.getName());

	private int streamId;
	private int portNumber;
	private int status = 0; // 0 idle; 1 started; 2 warning; 3 fatal

	// local copy of Stream mapping of which fields are number.
	private boolean numericFlags[];
	private int numberFieldCount;
	private int stringFieldCount;
	private int totalFieldCount;
	private int fieldMap[];

	private String fieldDelimiter = "\t";

	// An Inbox queue to receive Strings from the TCP socket
	private SpscChunkedArrayQueue<String> inboxQueue = new SpscChunkedArrayQueue<String>(QUEUE_INIT_CAPACITY,
			MAX_CAPACITY);

	private ServerSocket serverSocket;
	private Socket clientSocket;

	private Thread socketListenerThread;

	private boolean interrupt;
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

	@Override
	public RioDBStreamEvent getNextEvent() throws RioDBPluginException {

		String s = inboxQueue.poll();

		if (s != null && s.length() > 0) {
			s = s.trim();

			RioDBStreamEvent event = new RioDBStreamEvent(numberFieldCount, stringFieldCount);
			String fields[] = s.split(fieldDelimiter);

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
								logger.warn("Stream[" + streamId + "] received INVALID NUMBER [" + s + "]");
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
					logger.warn("Stream[" + streamId + "] received fewer values than expected. [" + s + "]");
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
		return "TCP";
	}

	@Override
	public void init(String datasourceParams, RioDBStreamEventDef def) throws RioDBPluginException {

		logger.info("initializing TCP plugin with paramters (" + datasourceParams + ")");

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

		String params[] = datasourceParams.split(" ");
		if (params.length < 2)
			throw new RioDBPluginException("Port parameter is required for plugin TCP.");

		// get port parameter
		boolean portNumberSet = false;
		String port = getParameter(params, "port");
		if (port != null) {
			if (isNumber(port)) {
				portNumber = Integer.valueOf(port);
				portNumberSet = true;
			} else {
				status = 3;
				throw new RioDBPluginException("Port number attribute must be numeric.");
			}
		}
		if (!portNumberSet)
			throw new RioDBPluginException("Port parameter is required for plugin TCP.");
		
		
		// get delimiter parameter
		String delimiter = getParameter(params, "delimiter");
		if (delimiter != null ) {
			if (delimiter.length() == 3) {
				fieldDelimiter = String.valueOf(delimiter.charAt(1));
			} else {
				status = 3;
				throw new RioDBPluginException("Syntax error. Delimiter should be specified in quotes:   DELIMITER ',' ");
			}
		}


		status = 0;

	}

	public void run() {
		logger.info("Starting TCP listener for stream[" + streamId + "] port " + portNumber);

		try {

			serverSocket = new ServerSocket(portNumber, DEFAULT_CONN_BACKLOG);
			// serverSocket.setSoTimeout(1000);

			// while (!interrupt) {

			// loop for accepting incoming connections.
			while ((clientSocket = serverSocket.accept()) != null && !interrupt) {
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
						if (received.equals(";"))
							break;
						inboxQueue.offer(received);
						counter++;
					}
					out.print("{\"received\":" + counter + "}");
					out.flush();
					in.close();
					out.close();
					clientSocket.close();
				} catch (IOException e) {
					logger.trace("TCP plugin : connection reset.");
				}
			}
			// }
			status = 0;
			if (!serverSocket.isClosed()) {
				logger.debug("Closing TCP server socket");
				serverSocket.close();
			}

		} catch (IOException e) {
			if (interrupt) {
				logger.info("Listener for stream[" + streamId + "] interrupted.");
				status = 0;
			} else {
				status = 3;
				logger.info("Listener for stream[" + streamId + "] interrupted unexpectedly!");
				logger.debug(e.getMessage().replace('\n', ';').replace('\r', ';'));
			}
		}

		logger.info("Listener for stream[" + streamId + "] stopped.");
	}

	@Override
	public void start() throws RioDBPluginException {
		interrupt = false;
		socketListenerThread = new Thread(this);
		socketListenerThread.setName("TCP_LISTENER_THREAD");
		socketListenerThread.start();
		status = 1;
	}

	public RioDBPluginStatus status() {
		return new RioDBPluginStatus(status);
	}

	@Override
	public void stop() {
		logger.debug("Closing TCP socket.");
		interrupt = true;
		try {
			if (!serverSocket.isClosed()) {
				logger.debug("Closing TCP server socket");
				serverSocket.close();
			}
		} catch (IOException e) {
			logger.error("Error closing TCP sockets: " + e.getMessage().replace("\n", " ").replace("\r", " "));
		}
		logger.debug("Interrupting TCP thread.");
		socketListenerThread.interrupt();
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