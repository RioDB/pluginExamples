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
import java.net.SocketException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jctools.queues.SpscChunkedArrayQueue;

public class UdpInput implements Runnable {

	// queue limits
	public static final int QUEUE_INIT_CAPACITY = 244; // 10000;
	public static final int MAX_CAPACITY = 1000000;

	// the max size of text msg
	private final int DEFAULT_BUFFER_SIZE = 1024;
	private int bufferSize = DEFAULT_BUFFER_SIZE;

	// loger
	private Logger logger = LogManager.getLogger(UDP.class.getName());

	// listener port number
	private int portNumber;

	// plugin status
	private int status = 0; // 0 idle; 1 started; 2 warning; 3 fatal

	// RioDB stream message def details.
	private boolean numericFlags[];
	private int numberFieldCount;
	private int stringFieldCount;
	private int totalFieldCount;
	private int fieldMap[];

	// delimiter separating fields.
	private String fieldDelimiter = "\t";
	private DateTimeFormatter timestampFormat = null;
	private int timestampFieldId = -1;
	private boolean timestampMillis = false;

	// An Inbox queue to receive stream raw data from TCP or UDP listeners
	private final SpscChunkedArrayQueue<DatagramPacket> streamPacketInbox = new SpscChunkedArrayQueue<DatagramPacket>(
			QUEUE_INIT_CAPACITY, MAX_CAPACITY);

	// instance of DatagramSocket
	private DatagramSocket socket;

	// Thread for running listener
	private Thread socketListenerThread;

	// boolean variable for interrupting thread while() loop.
	private boolean interrupt;

	// if an error occurs (like invalid number)
	// we only log it once.
	private boolean errorAlreadyCaught = false;

	// RioDB invokes this to poll the message queue.
	public RioDBStreamMessage getNextInputMessage() throws RioDBPluginException {

		// poll next packet from queue
		DatagramPacket packet = streamPacketInbox.poll();

		// if there is a non-null packet....
		if (packet != null) {

			// make a String object from packet
			String s = new String(packet.getData());

			if (s != null && s.length() > 0) {

				// s = s.trim(); no trim in case delimiter is spaces.

				// create new event.
				RioDBStreamMessage event = new RioDBStreamMessage(numberFieldCount, stringFieldCount);

				// split message fields by delimiter
				String fields[] = s.split(fieldDelimiter);

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
											logger.warn(UDP.PLUGIN_NAME + " INPUT field '"+ fields[i] +"' could not be parsed as '"+ timestampFormat.toString() +"'");
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
							logger.warn(UDP.PLUGIN_NAME + " INPUT received INVALID NUMBER [" + s + "]");
							errorAlreadyCaught = true;
						}
						return null;
					}

				} else {
					status = 2;
					if (!errorAlreadyCaught) {
						logger.warn(UDP.PLUGIN_NAME + " INPUT received fewer values than expected. [" + s + "]");
						errorAlreadyCaught = true;
					}
				}
			}
		}
		return null;
	}

	// get queue size
	public int getQueueSize() {
		return streamPacketInbox.size();
	}

	// initialize plugin for use as INPUT stream.
	public void initInput(String listenerParams, RioDBStreamMessageDef def) throws RioDBPluginException {

		logger.debug(UDP.PLUGIN_NAME + " initializing for INPUT with paramters (" + listenerParams + ")");

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
			logger.trace(UDP.PLUGIN_NAME + " timestampNumericFieldId is " + timestampFieldId);
		}
		
		if(def.getTimestampFormat() != null) {
			this.timestampFormat = DateTimeFormatter.ofPattern(def.getTimestampFormat());
			logger.trace(UDP.PLUGIN_NAME + " timestampFormat is " + timestampFormat);
		}

		if (def.getTimestampMillis()) {
			timestampMillis = true;
		}
			
			
			
		String params[] = listenerParams.split(" ");
		if (params.length < 2)
			throw new RioDBPluginException(UDP.PLUGIN_NAME + " input requires numeric 'port' parameter.");

		// get port parameter
		boolean portNumberSet = false;
		String port = getParameter(params, "port");
		if (port != null) {
			if (isNumber(port)) {
				portNumber = Integer.valueOf(port);
				portNumberSet = true;
			} else {
				status = 3;
				throw new RioDBPluginException(UDP.PLUGIN_NAME + " input requires numeric 'port' parameter.");
			}
		}
		if (!portNumberSet)
			throw new RioDBPluginException(UDP.PLUGIN_NAME + " input requires numeric 'port' parameter.");

		// get optional buffer_size
		String newBufferSize = getParameter(params, "buffer_size");
		if (newBufferSize != null) {
			if (isNumber(newBufferSize)) {
				bufferSize = Integer.valueOf(newBufferSize);
			} else {
				status = 3;
				throw new RioDBPluginException(UDP.PLUGIN_NAME + " input requires numeric 'buffer_size' parameter.");
			}

		}

		// get delimiter parameter
		String delimiter = getParameter(params, "delimiter");

		if (delimiter != null) {
			if (delimiter.length() == 3) {
				fieldDelimiter = String.valueOf(delimiter.charAt(1));

			} else {
				status = 3;
				throw new RioDBPluginException(
						UDP.PLUGIN_NAME + " input delimiter should be specified in quotes, like ',' ");
			}
		}

		status = 0;

	}

	public void run() {
		logger.debug(UDP.PLUGIN_NAME + " input starting on port " + portNumber);
		try {
			while (!interrupt) {// (!Thread.currentThread().isInterrupted()) {
				byte[] buf = new byte[bufferSize];
				DatagramPacket packet = new DatagramPacket(buf, bufferSize);
				socket.receive(packet);
				streamPacketInbox.offer(packet);
			}
		} catch (IOException e) {
			if (interrupt) {
				logger.debug(UDP.PLUGIN_NAME + " input port " + portNumber + " closed for stream.");
			} else {
				logger.error(UDP.PLUGIN_NAME + " input port " + portNumber + " closed unexpectedly!");
				logger.debug(e.getMessage().replace('\n', ';').replace('\r', ';'));
			}
		} finally {
			if (socket != null)
				socket.close();
		}
		status = 0;
	}

	public void start() throws RioDBPluginException {

		interrupt = false;
		try {
			socket = new DatagramSocket(portNumber);
			socketListenerThread = new Thread(this);
			socketListenerThread.setName(this.getClass().getName());
			socketListenerThread.start();
		} catch (SocketException e) {
			status = 3;
			RioDBPluginException p = new RioDBPluginException("Failed to start Listener. " + e.getMessage());
			p.setStackTrace(e.getStackTrace());
			throw p;
		}
		status = 1;
		logger.debug("UDP Input " + portNumber + " started.");
	}

	public RioDBPluginStatus status() {
		return new RioDBPluginStatus(status);
	}

	public void stop() {

		logger.debug("UDP Input closing socket " + portNumber);

		try {
			if (!socket.isClosed()) {
				socket.close();
			}
			Thread.sleep(10);
		} catch (InterruptedException e) {
			;
		}
		logger.debug("UDP Input interrupting Thread.");
		// socket.close();
		// socketListenerThread.interrupt();
		try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
			;
		}
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

	// local function to test if a string is a valid number.
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

}
