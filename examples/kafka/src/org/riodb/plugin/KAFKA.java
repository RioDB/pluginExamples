/*
			KAFKA   (www.riodb.org)

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
   	KAFKA is a RioDBPlugin that can be used as INPUT or OUTPUT.
   
   	INPUT: 
   	KAFKA subscribes to a topic on a kafka server
   
	OUTPUT
	KAFKA publishes to a topic on a kafka server
	
	
	www.riodb.org
  
 */

package org.riodb.plugin;

public class KAFKA implements RioDBPlugin {

	private KafkaInput input = new KafkaInput();

	public static final String PLUGIN_NAME = "KAFKA";
	public static final String VERSION = "0.1";
	private boolean isInput = true;

	@Override
	public RioDBStreamMessage getNextInputMessage() throws RioDBPluginException {
		if (isInput) {
			return input.getNextInputMessage();
		}
		return null;
	}

	@Override
	public int getQueueSize() {
		if (isInput) {
			return input.getQueueSize();
		}
		return 0;
	}

	@Override
	public String getType() {
		return PLUGIN_NAME;
	}

	@Override
	public void initInput(String inputParams, RioDBStreamMessageDef def) throws RioDBPluginException {

		input.init(inputParams, def);
	}

	@Override
	public void initOutput(String outputParams, String[] columnHeaders) throws RioDBPluginException {
		isInput = false;

	}

	@Override
	public void sendOutput(String[] columns) {
		if (!isInput) {

		}
	}

	@Override
	public void start() throws RioDBPluginException {

		if (isInput) {

			input.start();

		} else {

		}

	}

	@Override
	public RioDBPluginStatus status() {
		if (isInput) {
			return input.status();
		} else {
			return null;
		}
	}

	@Override
	public void stop() throws RioDBPluginException {
		if (isInput) {
			input.stop();
		} else {

		}

	}

	@Override
	public String version() {
		return VERSION;
	}

}
