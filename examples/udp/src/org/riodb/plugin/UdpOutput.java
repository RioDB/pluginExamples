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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UdpOutput {

	private Logger logger = LogManager.getLogger(UDP.class.getName());

	private int status = 0; // 0 idle; 1 started; 2 warning; 3 fatal

	private DatagramSocket socket;
	private InetAddress address;
	private int portNumber;

	private String delimiter = ",";
	
	public void initOutput(String outputParams, String[] columnHeaders) throws RioDBPluginException {

		logger.debug("initializing UDP plugin for OUTPUT with paramters ( " + outputParams + ") ");

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

		try {
			this.address = InetAddress.getByName(addressParam);
		} catch (UnknownHostException e) {
			throw new RioDBPluginException(
					"UDP OUTPUT: UnknownHostException while defining socket address: addressParam.");
		}

	}

	public void sendOutput(String[] columns) {
		String outStr = "";

		for (int i = 0; i < columns.length; i++) {
			if (i > 0) {
				outStr = outStr + delimiter;
			}
			outStr = outStr + columns[i];
		}

		byte[] buf = outStr.getBytes();
		DatagramPacket packet = new DatagramPacket(buf, buf.length, address, portNumber);
		try {
			socket.send(packet);
		} catch (IOException e) {
			if (status == 0) {
				logger.error("UDP OUTPUT IOException.");
				logger.debug(e.getMessage().replace("\n", " ").replace("\r", " ").replace("\"", " "));
			}
			status = 2;
		}

	}

	public void start() throws RioDBPluginException {
		try {
			this.socket = new DatagramSocket();
		} catch (SocketException e) {
			status = 3;
			throw new RioDBPluginException("UDP OUTPUT: SocketException while starting plugin");
		}
		status = 0;
	}

	public RioDBPluginStatus status() {
		return new RioDBPluginStatus(status);
	}

	public void stop() {
		if (!socket.isClosed()) {
			socket.close();
		}
	}

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
}
