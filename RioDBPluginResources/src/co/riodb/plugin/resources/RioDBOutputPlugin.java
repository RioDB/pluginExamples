/*-
 * Copyright (c) 2025 Lucio D Matos - www.riodb.co
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package co.riodb.plugin.resources;

public abstract class RioDBOutputPlugin extends RioDBPlugin {

	public RioDBOutputPlugin(LoggingService loggingService) {
		super(loggingService);
	}

	// method for checking the data source awaiting event queue size.
	public abstract int getQueueSize();

	// initialize plugin to be used as query OUTPUT
	public abstract void init(String outputParams) throws RioDBPluginException;

	// Send output using an array of selected columns
	public abstract void sendOutput(String message);

	// starting the data source (most use a Runnable Thread)
	public abstract void start() throws RioDBPluginException;

	// stop the data source (most use a Runnable thread)
	public abstract void stop() throws RioDBPluginException;

}
