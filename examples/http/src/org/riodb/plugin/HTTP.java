/*
			HTTP   (www.riodb.org)

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
   	HTTP is a RioDBPlugin that can be used as INPUT or OUTPUT.
   
   	INPUT: 
   	HTTP receives messages using java HttpServer
   
	OUTPUT
	HTTP sends message to a specified destination
	using java HttpClient. 
	
	
	www.riodb.org
  
 */

package org.riodb.plugin;

public class HTTP implements RioDBPlugin {
	
	// Plugin Name
	public static final String PLUGIN_NAME = "HTTP";
	// Plugin Version
	public static final String VERSION = "0.0.3";
	
	// a class with methods for using UDP as input
	private final HttpInput input = new HttpInput();
	// a class with methods for using UDP as output
	private final HttpOutput output = new HttpOutput();
	// a flag to determine what "this" plugin is for
	// (to help direct start, stop, status functions)
	private boolean isInput = true;

	@Override
	public String getType() {
		return PLUGIN_NAME;
	}
	
	@Override
	public String version() {
		return VERSION;
	}

	@Override
	public void start() throws RioDBPluginException {
		if(isInput) {
			input.start();
		}
		output.start();
	}

	@Override
	public RioDBPluginStatus status() {
		if(isInput) {
			return input.status();
		}
		return output.status();
	}

	@Override
	public void stop() throws RioDBPluginException {
		if(isInput) {
			input.stop();
		}
		output.stop();
	}
	
	
	
	/*
	 *   Methods for INPUT usage
	 */
	
	@Override
	public RioDBStreamMessage getNextInputMessage() throws RioDBPluginException {
		return input.getNextInputMessage();
	}

	@Override
	public int getQueueSize() {
		if(isInput) {
			return input.getQueueSize();
		}
		return 0;
	}


	@Override
	public void initInput(String inputParams, RioDBStreamMessageDef def) throws RioDBPluginException {
		input.initInput(inputParams, def);
	}
	
	
	

	/*
	 *   Methods for OUTPUT usage
	 */
	
	@Override
	public void initOutput(String outputParams, String[] columnHeaders) throws RioDBPluginException {
		isInput = false;
		output.initOutput(outputParams, columnHeaders);
	}

	@Override
	public void sendOutput(String[] columns) {
		output.sendOutput(columns);
	}
}