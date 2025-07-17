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

public abstract class RioDBFormatterPlugin extends RioDBPlugin {

	public RioDBFormatterPlugin(LoggingService loggingService) {
		super(loggingService);
	}

	// Returns parsed fields of multiple records as list of arrays of strings.
	public abstract String format(String columns[]) throws RioDBPluginException;

	// initializer
	public abstract void init(String params, String[] columnHeaders) throws RioDBPluginException;

}
