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

// plugin class, which is extended as input, output, parser, formatter.
public class RioDBPlugin {

	// version
	private final String PLUGIN_VERSION = "0.5";

	// log prefix string
	private final String PLUGIN_TYPE = this.getClass().getName().replace("co.riodb.plugin.", "");

	// log service is dependency injection sent from RioDB
	private final LoggingService loggingService;

	// status of this plugin
	protected final RioDBPluginStatus pluginStatus = new RioDBPluginStatus(0);

	// constructor receives logging class from RioDB
	public RioDBPlugin(LoggingService loggingService) {
		this.loggingService = loggingService;
	}

	// method that RioDB may call to reset warnings
	public void clearWarnings() {
		if (pluginStatus != null && !pluginStatus.getStatus().equals("FATAL")) {
			pluginStatus.setOk();
		}
	}

	public String describe() {
		String s = "{\"plugin_type\":\"" + PLUGIN_TYPE + "\",\"plugin_version\":\"" + PLUGIN_VERSION
				+ "\",\"plugin_status\":\"" + (pluginStatus != null ? pluginStatus.getStatusCode() : null) + "\"}";
		return s;
	}

	public RioDBPluginStatus getStatus() {
		return pluginStatus;
	}

	// Gets plugin type
	public String getType() {
		return PLUGIN_TYPE;
	}

	public String getVersion() {
		return PLUGIN_VERSION;
	}

	protected void logDebug(String msg) {
		loggingService.logDebug(PLUGIN_TYPE + " - ", msg);
	}

	protected void logError(String msg) {
		loggingService.logError(PLUGIN_TYPE + " - ", msg);
	}

	protected void logFatal(String msg) {
		loggingService.logFatal(PLUGIN_TYPE + " - ", msg);
	}

	protected void logInfo(String msg) {
		loggingService.logInfo(PLUGIN_TYPE + " - ", msg);
	}

	protected void logTrace(String msg) {
		loggingService.logTrace(PLUGIN_TYPE + " - ", msg);
	}

	protected void logWarn(String msg) {
		loggingService.logWarn(PLUGIN_TYPE + " - ", msg);
	}

}
