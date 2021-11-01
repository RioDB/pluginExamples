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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TcpOutput {
	
	
	private Logger logger = LogManager.getLogger(TCP.class.getName());

	private int status = 0; // 0 idle; 1 started; 2 warning; 3 fatal
	
	private String address = null;
	private int portNumber;
	private String delimiter = ",";
	
	public void init(String outputParams, String[] columnHeaders) throws RioDBPluginException {

		logger.debug("initializing TCP plugin for OUTPUT with paramters ( " + outputParams + ") ");

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
		
	}

	public void sendOutput(String[] columns) {
		
		   String msg = "";

			for (int i = 0; i < columns.length; i++) {
				if (i > 0) {
					msg = msg + delimiter;
				}
				msg = msg + columns[i];
			}
			
			try (Socket socket = new Socket(address, portNumber)) {
	   		 
	            OutputStream output = socket.getOutputStream();
	            PrintWriter writer = new PrintWriter(output, true);
	            writer.println(msg);
	            output.flush();
	            output.close();
	            socket.close();

	        } catch (UnknownHostException e) {
	 
	        	if (status == 0) {
					logger.error("TCP OUTPUT UnknownHostException");
					logger.debug(e.getMessage().replace("\n", " ").replace("\r", " ").replace("\"", " "));
				}
				status = 2;
	 
	        } catch (IOException e) {
	 
	        	if (status == 0) {
					logger.error("TCP OUTPUT IOException");
					logger.debug(e.getMessage().replace("\n", " ").replace("\r", " ").replace("\"", " "));
				}
				status = 2;
	        }
    	
	}

	
	public void start() {
		status = 1;
	}

	public RioDBPluginStatus status() {
		return new RioDBPluginStatus(status);
	}
	
	public void stop() {
		status = 0;
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
