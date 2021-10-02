/*
				BENCH   (www.riodb.org)

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
   BENCH is a plugin for bench testing. It just produces numbers in a loop
   and adds them to a queue for RioDB stream to take from.  
   
   To compile, install maven and run this from the root directory where pom.xml is:
   
   mvn clean compile assembly:single
   
   The file target/bench-jar-with-dependencies.jar will be crated
   
   rename it to bench.jar and copy it to RioDB's /plugins directory
   (or whatever directory RioDB will search for plugins)
   
   www.riodb.org
   
   
 */

package org.riodb.plugin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jctools.queues.SpscChunkedArrayQueue;

public class BENCH implements RioDBPlugin, Runnable {
	
	public static final String PLUGIN_NAME = "BENCH";
	
	// queue limits
	private static final int QUEUE_INIT_CAPACITY = 244; // 10000;
	private static final int MAX_CAPACITY = 100000;
	private static final int DEFAULT_CEILING = 1000000000;

	// logger
	private Logger logger = LogManager.getLogger("RIO.LOG");

	// private int streamId;
	private int status = 0; // 0 idle; 1 started; 2 warning; 3 fatal

	// local copy of Stream mapping of which fields are number.
	private boolean numericFlags[];
	
	// total number of numeric fields
	private int numberFieldCount;
	
	// total number of string fields
	private int stringFieldCount;
	
	// total number of fields;
	private int totalFieldCount;
	
	// field map
	private int fieldMap[];
	
	// ceiling for the LOOP, when loop starts over.
	private int ceiling = DEFAULT_CEILING;
	
	// array of strings to alternate in string fields. 
	private String strings[] = { "" };
	
	// should pause in between generated messages? 
	private boolean doIntervals = false;
	
	// pause length in milliseconds
	private int intervalMillis = 0;
	
	// increment numeric values by...
	private int increment = 1;

	
	// An Inbox queue to receive Strings from the TCP socket
	private final SpscChunkedArrayQueue<RioDBStreamMessage> inboxQueue = new SpscChunkedArrayQueue<RioDBStreamMessage>(
			QUEUE_INIT_CAPACITY, MAX_CAPACITY);

	private Thread benchThread;

	private boolean interrupted;

	private static final boolean isNumber(String s) {
		if (s != null)
			try {
				Float.valueOf(s);
				return true;
			} catch (NumberFormatException nfe) {
				return false;
			}
		else
			return false;
	}

	@Override
	public RioDBStreamMessage getNextInputMessage() throws RioDBPluginException {
		return inboxQueue.poll();
	}

	// function to get parameter from an array of strings. 
	// for a parameter key in params[i], the parameter value
	// is in params[i+1]
	private String getParameter(String params[], String key) {
		for (int i = 0; i < params.length; i++) {
			if (key.equals(params[i].toLowerCase()) && i < params.length - 1) {
				return params[i + 1];
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
		return "BENCH";
	}

	// initialize bench as input. 
	@Override
	public void initInput(String inputParams, RioDBStreamMessageDef def) throws RioDBPluginException {
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

		String params[] = inputParams.split(" ");
		String ceilingParam = getParameter(params, "ceiling");
		if (ceilingParam != null && isNumber(ceilingParam)) {
			ceiling = Integer.valueOf(ceilingParam);
		}

		String stringsParam = getParameter(params, "strings");
		if (stringsParam != null) {
			strings = stringsParam.replace("'", "").split("\\|");
		}

		String incrementParam = getParameter(params, "increment");
		if (incrementParam != null && isNumber(incrementParam)) {
			increment = Integer.valueOf(incrementParam);
		}

		String intervalParam = getParameter(params, "interval");
		if (intervalParam != null && isNumber(intervalParam)) {
			doIntervals = true;
			intervalMillis = Integer.valueOf(intervalParam);
		}

		status = 0;
		
		logger.debug(PLUGIN_NAME+" is initialized.");
	}

	@Override
	public void initOutput(String outputParams, String[] columnHeaders) throws RioDBPluginException {
		throw new RioDBPluginException("BENCH plugin cannot be used for OUTPUT.");
	}

	@Override
	public void run() {
		logger.debug(PLUGIN_NAME+"BENCH is running...");

		double[] nums = new double[numberFieldCount];
		for (int i = 0; i < nums.length; i++) {
			nums[i] = 0;
		}

		int stringsMarker = 0;

		while (!interrupted) {

			// if user wants intervals in between messages
			if (doIntervals) {
				try {
					Thread.sleep(intervalMillis);
				} catch (InterruptedException e) {
					;
				}
			}

			// create new event object
			RioDBStreamMessage message = new RioDBStreamMessage(numberFieldCount, stringFieldCount);
			// populate event fields with data.
			for (int i = totalFieldCount - 1; i >= 0; i--) {
				if (numericFlags[i]) {
					message.set(fieldMap[i], nums[fieldMap[i]]);
				} else {
					message.set(fieldMap[i], strings[stringsMarker]);
				}
			}
			if (++stringsMarker == strings.length) {
				stringsMarker = 0;
			}
			// attempt to insert event into queue. It will return false if the queue is full
			// to max capacity.
			if (inboxQueue.offer(message)) {
				// event queued. Increment number variables.
				nums[0] += increment;
				for (int i = 0; i < nums.length; i++) {
					if (nums[i] >= ceiling) {
						nums[i] = 0;
						if (i < nums.length - 1) {
							nums[i + 1] += increment;
						} else { // reset
							if (nums.length > 1) {
								for (int j = 0; j < nums.length; j++) {
									nums[j] = 0;
								}
							}
						}
					}

				}
			} else { // try again in a bit. Same numbers.
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
//							logger.error("BENCH plugin for stream[" + streamId + "] - error sleeping.");
				}
			}

		}

//		logger.info("Listener for stream[" + streamId + "] stopped.");
	}

	@Override
	public void sendOutput(String[] columns) {
		// not needed. bench is not used for output.
	}

	@Override
	public void start() throws RioDBPluginException {
		interrupted = false;
		benchThread = new Thread(this);
		benchThread.setName("BENCH_THREAD");
		benchThread.start();
		status = 1;
		logger.debug(PLUGIN_NAME+"BENCH is started.");
	}

	@Override
	public RioDBPluginStatus status() {
		return new RioDBPluginStatus(status);
	}

	@Override
	public void stop() {
		interrupted = true;
		benchThread.interrupt();
		status = 0;
		logger.debug(PLUGIN_NAME+"BENCH is stopped.");
	}
}