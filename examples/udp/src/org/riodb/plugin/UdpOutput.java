/*
			UDP   (www.riodb.org)

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
   	UDP is a RioDBPlugin that can be used as INPUT or OUTPUT.
   
   	INPUT: 
   
   	UDP plugin listens as a socket server.
   	It receives lines of text via UDP connection
   	and stores them in a queue. 
   	RioDB stream process can poll events from the queue. 
   
	OUTPUT
	
	UDP plugin sends message to a specified destination
	using UDP protocol. 
	
	www.riodb.org
  
  
 */
package org.riodb.plugin;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jctools.queues.SpscChunkedArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UdpOutput {

	// default queue limits
	public static final int DEFAULT_QUEUE_INIT_CAPACITY = 244; // 10000;
	public static final int DEFAULT_MAX_CAPACITY = 1000000;

	// logger
	private Logger logger = LoggerFactory.getLogger("org.riodb.plugin." + UDP.PLUGIN_NAME);
	private String logPrefix = UDP.PLUGIN_NAME + " UdpOutput - ";

	private int status = 0; // 0 idle; 1 started; 2 warning; 3 fatal

	// running in mode EFFICIENT vs EXTREME
	private boolean extremeMode = false;

	// An Outgoing queue based on arrayQueue for extreme loads
	private SpscChunkedArrayQueue<String[]> outgoingArrayQueue;

	// An Outgoing queue based on blockingQueue for efficient processing
	private LinkedBlockingQueue<String[]> outgoingBlockingQueue;

	// boolean variable for interrupting thread while() loop.
	private AtomicBoolean interrupt;
	
	// a poison pill to interrupt blocked take()
	private final String[] POISON = { "'", "!" };

	// if an error occurs (like invalid number)
	// we only log it once.
	private boolean errorAlreadyCaught = false;

	// socket details for sending UDP message
	private DatagramSocket socket;
	private InetAddress address;
	private int portNumber;

	// delimiter used to separate columns in output.
	private String delimiter = ",";

	// local function to get parameter values from initializing string
	// (The init string is the text in parenthesis in the SELECT .. OUTPUT
	// statement.
	private static final String getParameter(String params[], String key) {
		for (int i = 0; i < params.length; i++) {
			if (key.equals(params[i].toLowerCase()) && i < params.length - 1) {
				return params[i + 1];
			}
		}
		return null;
	}

	// local function to determine if a string is a number
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
		if (extremeMode) {
			return outgoingBlockingQueue.size();
		}
		return outgoingArrayQueue.size();
	}

	// initialize plugin for use as OUTPUT
	public void initOutput(String outputParams, String[] columnHeaders) throws RioDBPluginException {

		logger.debug(logPrefix + "Initializing with paramters ( " + outputParams + ") ");

		// output parameters are provided by user in SQL statement (everything between
		// parenthesis)
		String params[] = outputParams.split(" ");

		// output destination
		String addressParam = getParameter(params, "address");
		String portParam = getParameter(params, "port");

		if (addressParam == null || portParam == null) {
			throw new RioDBPluginException("ADDRESS and PORT parameters are required.");
		}
		addressParam = addressParam.replace("'", "");
		portParam = portParam.replace("'", "");

		if (!isNumber(portParam) || Integer.valueOf(portParam) < 1) {
			throw new RioDBPluginException("PORT parameter must be a positive integer.");
		}
		this.portNumber = Integer.valueOf(portParam);

		// field delimiter
		String delimiterParam = getParameter(params, "delimiter");
		if (delimiterParam != null) {
			delimiter = delimiterParam.replace("'", "");
		}

		int maxCapacity = DEFAULT_MAX_CAPACITY;
		// get optional queue capacity
		String newMaxCapacity = getParameter(params, "queue_capacity");
		if (newMaxCapacity != null) {
			if (isNumber(newMaxCapacity) && Integer.valueOf(newMaxCapacity) > 0) {
				maxCapacity = Integer.valueOf(newMaxCapacity);
			} else {
				status = 3;
				throw new RioDBPluginException("Requires positive intenger for 'queue_capacity' parameter.");
			}

		}

		// if user opted for extreme mode...
		String mode = getParameter(params, "mode");
		if (mode != null && mode.equals("extreme")) {
			extremeMode = true;
			outgoingArrayQueue = new SpscChunkedArrayQueue<String[]>(DEFAULT_QUEUE_INIT_CAPACITY, maxCapacity);
		} else {
			outgoingBlockingQueue = new LinkedBlockingQueue<String[]>(maxCapacity);
		}

		interrupt = new AtomicBoolean(true);

		try {
			this.address = InetAddress.getByName(addressParam);
		} catch (UnknownHostException e) {
			throw new RioDBPluginException("UnknownHostException while defining socket address: addressParam.");
		}
		
		logger.debug(logPrefix +"Initialized.");

	}

	private DatagramPacket makeDatagramPacket(String[] columns) {

		StringBuilder outStr = new StringBuilder("");

		// concatenate fields split by delimiter.
		for (int i = 0; i < columns.length; i++) {
			if (i > 0) {
				outStr.append(delimiter);
			}
			outStr.append(columns[i]);
		}

		// prepare data for sucket.
		byte[] buf = outStr.toString().getBytes();
		return new DatagramPacket(buf, buf.length, address, portNumber);

	}

	// procedure to send UDP output to destination.
	public void sendOutput(String[] columns) {

		// send to appropriate queue.
		if (!(extremeMode ? outgoingArrayQueue.offer(columns) : outgoingBlockingQueue.offer(columns))) {
			// offer returns false when the queue is full. So if above is FALSE, then the
			// queue is full.
			if (!errorAlreadyCaught) {
				logger.error(logPrefix + "Outgoing queue is full! New outgoing messages are being dropped. Limit is "
						+ DEFAULT_MAX_CAPACITY);
				errorAlreadyCaught = true;
			}
		}
	}

	// procedure for sending UDP packet to socket.
	private void sendUDPpacket(DatagramPacket packet) {
		try {
			socket.send(packet);
		} catch (IOException e) {
			if (!errorAlreadyCaught) {
				logger.error(logPrefix + "IOException.");
				logger.debug(logPrefix + e.getMessage().replace("\n", " ").replace("\r", " ").replace("\"", " "));
				errorAlreadyCaught = true;
				status = 2;
			}
		}
	}

	// procedure for starting the output plugin thread.
	public void start() throws RioDBPluginException {

		logger.debug(logPrefix + "Starting...");
		errorAlreadyCaught = false;
		interrupt.set(false);
		try {
			this.socket = new DatagramSocket();
		} catch (SocketException e) {
			interrupt.set(true);
			status = 3;
			throw new RioDBPluginException(e.getMessage());
		}
		status = 1;

		// again, extremeMode works with arrayDeque,
		// and efficient mode works with blockingQueue

		while (!interrupt.get()) {

			String columns[];

			// if running extreme mode.
			if (extremeMode) {

				// we poll the arraydequeue. Could be empty.
				columns = outgoingArrayQueue.poll();
				// if empty, we do a wait 1ms and try again.
				while ((columns == null || columns.length == 0) && !interrupt.get()) {
					// wait and try again until not null
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						;
					}
					columns = outgoingArrayQueue.poll();
				}

			} else { // efficient mode - blockingQueue

				// a quick poll in case queue is not empty
				columns = outgoingBlockingQueue.poll();

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
			}

			if (columns != null && columns.length > 0 && !interrupt.get() && columns != POISON) {
				DatagramPacket packet = makeDatagramPacket(columns);
				sendUDPpacket(packet);
			}
		}

		logger.debug(logPrefix + "Closing socket...");
		this.socket.close();
		errorAlreadyCaught = false;
		status = 0;
		logger.debug(logPrefix + "Stopped.");
	}

	// getter for plugin status
	public RioDBPluginStatus status() {
		return new RioDBPluginStatus(status);
	}

	// procedure for stopping output plugin thread.
	public void stop() {
		logger.debug(logPrefix + "Stopping...");
		interrupt.set(true);
		if (!extremeMode) {
			outgoingBlockingQueue.offer(POISON);
		}
	}
}
