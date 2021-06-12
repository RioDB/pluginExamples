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

package org.riodb.plugin;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
import org.jctools.queues.SpscChunkedArrayQueue;

public class UDP implements RioDBDataSource, Runnable {
	public static final int QUEUE_INIT_CAPACITY = 244; // 10000;
	public static final int MAX_CAPACITY = 1000000;
	/// the max size of text msg
	private final int DEFAULT_BUFFER_SIZE = 1024;
	private int bufferSize = DEFAULT_BUFFER_SIZE;
	
//	Logger logger = LogManager.getLogger("RIO.LOG");

//	private int streamId;
	private int portNumber;
	private int status = 0; // 0 idle; 1 started; 2 warning; 3 fatal

	// local copy of Stream mapping of which fields are number.
	private boolean numericFlags[];
	private int numberFieldCount;
	private int stringFieldCount;
	private int totalFieldCount;
	private int fieldMap[];

	// An Inbox queue to receive stream raw data from TCP or UDP listeners
	private final SpscChunkedArrayQueue<DatagramPacket> streamPacketInbox = new SpscChunkedArrayQueue<DatagramPacket>(
			QUEUE_INIT_CAPACITY, MAX_CAPACITY);

	private DatagramSocket socket;

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

		DatagramPacket packet = streamPacketInbox.poll();

		if (packet != null) {
			//String s = new String(packet.getData(), 0, packet.getLength());
			String s = new String(packet.getData());

//			System.out.println("IN: "+s);

			if (s != null && s.length() > 0) {
				s = s.trim();

				RioDBStreamEvent event = new RioDBStreamEvent(numberFieldCount, stringFieldCount);
				String fields[] = s.split("\t");

				if (fields.length >= totalFieldCount) {

					try {

						for (int i = 0; i < totalFieldCount; i++) {
							if (numericFlags[i]) {
								event.set(fieldMap[i], Double.valueOf(fields[i]));
							} else {
								event.set(fieldMap[i], fields[i]);
							}
						}
						return event;
						
					} catch (NumberFormatException nfe) {
						status = 2;
						if (!errorAlreadyCaught) {
//							logger.warn("Stream[" + streamId + "] received INVALID NUMBER [" + s + "]");
							errorAlreadyCaught = true;
						}
						return null;
					}


				} else {
					status = 2;
					if (!errorAlreadyCaught) {
//						logger.warn("Stream[" + streamId + "] received fewer values than expected. [" + s + "]");
						errorAlreadyCaught = true;
					}
				}
			}
		}
		return null;
	}

	@Override
	public int getQueueSize() {
		return streamPacketInbox.size();
	}

	@Override
	public String getType() {
		return "UDP";
	}

	@Override
	public void init(String listenerParams, RioDBStreamEventDef def) throws RioDBPluginException {

//		logger.info("initializing UDP plugin with paramters (" + listenerParams + ")");

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

		
		String params[] = listenerParams.split(" ");
		if(params.length < 2 )
			throw new RioDBPluginException("Port parameter is required for plugin UDP.");

		boolean portNumberSet = false;
		for(int i = 0; i< params.length; i++) {
			if(params[i].toLowerCase().equals("port")){
				if (isNumber(params[i+1])) {
					portNumber = Integer.valueOf(params[i+1]);
					portNumberSet = true;
				} else {
					status = 3;
					throw new RioDBPluginException("Port number attribute must be numeric.");
				}
			}
			else if(params[i].toLowerCase().equals("buffer_size")){
				if (isNumber(params[i+1])) {
					bufferSize = Integer.valueOf(params[i+1]);
				} else {
					status = 3;
					throw new RioDBPluginException("buffer_size parameter must be numeric.");
				}
			}
		}
		if(!portNumberSet)
			throw new RioDBPluginException("Port parameter is required for plugin UDP.");
		

		status = 0;

	}

	public void run() {
//		logger.info("Starting UDP listener for stream[" + streamId + "] port " + portNumber);
		try {
			while (!interrupt) {// (!Thread.currentThread().isInterrupted()) {
				byte[] buf = new byte[bufferSize];
				DatagramPacket packet = new DatagramPacket(buf, bufferSize);
				socket.receive(packet);
				streamPacketInbox.offer(packet);
			}
		} catch (IOException e) {
			if (interrupt) {
//				logger.info("Listener connections for stream[" + streamId + "] closed.");
			} else {
//				logger.error("Listener connections for stream[" + streamId + "] closed unexpectedly!");
//				logger.error(e.getMessage().replace('\n', ';').replace('\r', ';'));
			}
		} finally {
			if (socket != null)
				socket.close();
		}
		status = 0;
//		logger.info("Listener for stream[" + streamId + "] stopped.");
	}

	@Override
	public void start() throws RioDBPluginException {
		
		interrupt = false;
		try {
			socket = new DatagramSocket(portNumber);
			socketListenerThread = new Thread(this);
			socketListenerThread.setName("UDP_LISTENER_THREAD");
			socketListenerThread.start();
		} catch (SocketException e) {
			status = 3;
			RioDBPluginException p = new RioDBPluginException("Failed to start Listener. " + e.getMessage());
			p.setStackTrace(e.getStackTrace());
			throw p;
		}
		status = 1;
	}

	public RioDBPluginStatus status() {
		return new RioDBPluginStatus(status);
	}

	@Override
	public void stop() {
		interrupt = true;
		socket.close();
		socketListenerThread.interrupt();
		status = 0;
	}

}