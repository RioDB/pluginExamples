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
   
   NOTES:
   This TcpInput is single-threaded and does not support pipelining
   Delimiters can be specified, but not fixed-width.
   
 */
package org.riodb.plugin;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpOutput {

	// default queue limits
	public static final int DEFAULT_QUEUE_INIT_CAPACITY = 244; // 10000;
	public static final int DEFAULT_MAX_CAPACITY = 1000000;

	// logger
	private Logger logger = LoggerFactory.getLogger("org.riodb.plugin." + TCP.PLUGIN_NAME);
	private String logPrefix = "TcpOutput - ";

	private int status = 0; // 0 idle; 1 started; 2 warning; 3 fatal

	private String address = null;
	private int portNumber;
	private String delimiter = ",";

	// An Outgoing queue based on blockingQueue for efficient processing
	private LinkedBlockingQueue<String[]> outgoingBlockingQueue;

	// boolean variable for interrupting thread while() loop.
	private AtomicBoolean interrupt;

	// a poison pill to interrupt blocked take()
	private final String[] POISON = { "'", "!" };

	// if an error occurs (like invalid number)
	// we only log it once.
	private boolean errorAlreadyCaught = false;

	// Inner class for output workers

	private static final String getParameter(String params[], String key) {
		for (int i = 0; i < params.length; i++) {
			if (key.equals(params[i].toLowerCase()) && i < params.length - 1) {
				return params[i + 1];
			}
		}
		return null;
	}

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

	// get queue size
	public int getQueueSize() {

		return outgoingBlockingQueue.size();

	}

	public void init(String outputParams, String[] columnHeaders) throws RioDBPluginException {

		logger.debug(logPrefix + "Initializing with paramters ( " + outputParams + ") ");

		String params[] = outputParams.split(" ");

		String addressParam = getParameter(params, "address");
		String portParam = getParameter(params, "port");

		if (addressParam == null || portParam == null) {
			throw new RioDBPluginException("ADDRESS and PORT parameters are required to use UDP OUTPUT.");
		}
		addressParam = addressParam.replace("'", "");
		portParam = portParam.replace("'", "");

		if (!isNumber(portParam) || Integer.valueOf(portParam) < 1) {
			throw new RioDBPluginException("PORT parameter must be a positive integer.");
		}
		this.portNumber = Integer.valueOf(portParam);

		String delimiterParam = getParameter(params, "delimiter");
		if (delimiterParam != null) {
			delimiter = delimiterParam.replace("'", "");
		}

		this.address = addressParam;

		int maxCapacity = DEFAULT_MAX_CAPACITY;
		// get optional queue capacity
		String newMaxCapacity = getParameter(params, "queue_capacity");
		if (newMaxCapacity != null) {
			if (isNumber(newMaxCapacity) && Integer.valueOf(newMaxCapacity) > 0) {
				maxCapacity = Integer.valueOf(newMaxCapacity);
			} else {
				status = 3;
				throw new RioDBPluginException(
						TCP.PLUGIN_NAME + " output requires positive intenger for 'queue_capacity' parameter.");
			}

		}

		outgoingBlockingQueue = new LinkedBlockingQueue<String[]>(maxCapacity);

		interrupt = new AtomicBoolean(true);

		logger.debug(logPrefix + "Initialized.");

	}

	// method for sending columns to output worker(s)
	public void sendOutput(String[] columns) {

		outgoingBlockingQueue.offer(columns);

	}

	// procedure for sending UDP packet to socket.
	private void sendTCPmsg(String columns[]) {

		StringBuilder msg = new StringBuilder("");

		for (int i = 0; i < columns.length; i++) {
			if (i > 0) {
				msg.append(delimiter);
			}
			msg.append(columns[i]);
		}

		try (Socket socket = new Socket(address, portNumber)) {

			OutputStream output = socket.getOutputStream();
			PrintWriter writer = new PrintWriter(output, true);
			writer.println(msg.toString());
			output.flush();
			output.close();
			socket.close();

		} catch (UnknownHostException e) {

			if (!errorAlreadyCaught) {
				errorAlreadyCaught = true;
				logger.error(
						logPrefix + "Worker Thread [" + Thread.currentThread().getName() + "]  UnknownHostException");
				logger.debug(e.getMessage().replace("\n", " ").replace("\r", " ").replace("\"", " "));
			}
			status = 2;

		} catch (IOException e) {

			if (!errorAlreadyCaught) {
				errorAlreadyCaught = true;
				logger.error(logPrefix + "Worker Thread [" + Thread.currentThread().getName() + "] IOException");
				logger.debug(e.getMessage().replace("\n", " ").replace("\r", " ").replace("\"", " "));
			}
			status = 2;
		}

	}

	public void start() {

		errorAlreadyCaught = false;

		logger.debug(logPrefix + "Starting...");
		interrupt.set(false);
		status = 1;

		while (!interrupt.get()) {

			// a quick poll in case queue is not empty
			String columns[] = outgoingBlockingQueue.poll();

			// if queue was empty
			if (columns == null || columns.length == 0) {

				/// then we take() and wait for something to be received.
				try {
					columns = outgoingBlockingQueue.take();
				} catch (InterruptedException e) {
					logger.debug(logPrefix + "Blocking queue interrupted.");
					break;
				}
			}

			if (columns != null && columns.length > 0 && columns != POISON) {
				sendTCPmsg(columns);
			}
		}

		logger.debug(logPrefix + "Stopped.");

		status = 0;

	}

	public RioDBPluginStatus status() {
		return new RioDBPluginStatus(status);
	}

	public void stop() {
		logger.debug(logPrefix + "Stopping...");
		interrupt.set(true);
		outgoingBlockingQueue.offer(POISON);

	}

}
