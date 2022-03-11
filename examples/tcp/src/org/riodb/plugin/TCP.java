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
   	TCP is a RioDBPlugin that can be used as INPUT or OUTPUT.
   
   	INPUT: 
   
   	TCP plugin listens as a socket server.
   	It receives lines of text via TCP connection
   	and stores them in a queue. 
   	RioDB stream process can poll events from the queue. 
   
	OUTPUT
	
	TCP plugin sends message to a specified destination
	using TCP protocol. 
	
	www.riodb.org
  
  
 */

package org.riodb.plugin;

public class TCP implements RioDBPlugin {

	// plugin name
	public static final String PLUGIN_NAME = "TCP";
	
	// plugin version. Preferrably matching RioDBPlugin version.
	public static final String VERSION = "0.0.4";

	// a class with methods for using TCP as input
	private final TcpInput input = new TcpInput();
	
	// a class with methods for using UDP as output
	private final TcpOutput output = new TcpOutput();
	
	// a flag to determine which use "this" plugin is for
	private boolean isInput = true;

	@Override
	public int getQueueSize() {
		if(isInput) {
			return input.getQueueSize();
		}
		return output.getQueueSize();
	}
	
	@Override
	public String getType() {
		return PLUGIN_NAME;
	}

	@Override
	public void start() throws RioDBPluginException {
		if(isInput) {
			input.start();
		} else {
			output.start();
		}
	}

	@Override
	public RioDBPluginStatus status() {
		if(isInput) {
			return input.status();
		} else {
			return output.status();
		}
	}

	@Override
	public void stop() {
		if(isInput) {
			input.stop();
		} else {
			output.stop();
		}
	}
	
	@Override
	public String version() {
		return VERSION;
	}
	

	
	/*
 	*   Methods for INPUT usage
 	*/

	@Override
	public RioDBStreamMessage getNextInputMessage() throws RioDBPluginException {
		return input.getNextInputMessage();
	}


	@Override
	public void initInput(String inputParams, RioDBStreamMessageDef def) throws RioDBPluginException {
		input.init(inputParams, def);

		// throw new RioDBPluginException("TCP plugin does not support input methods.");
	}
	

	/*
	 *   Methods for OUTPUT usage
	 */
	
	@Override
	public void initOutput(String outputParams, String[] columnHeaders) throws RioDBPluginException {
		isInput = false;
		output.init(outputParams, columnHeaders);
		//throw new RioDBPluginException("TCP plugin does not support output methods.");
	}

	@Override
	public void sendOutput(String[] columns) {
		output.sendOutput(columns);
	}

}