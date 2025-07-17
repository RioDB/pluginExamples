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

import java.util.LinkedList;

public abstract class RioDBParserPlugin extends RioDBPlugin {

	// default Plugin constructor that gets LoggingService injected from RioDB
	public RioDBParserPlugin(LoggingService loggingService) {
		super(loggingService);
	}

	// initializer (parameters sent to the parser and expected field count)
	public abstract void init(String params, int fieldCount) throws RioDBPluginException;

	// Returns parsed fields of multiple records as list of arrays of strings.
	public abstract LinkedList<String[]> parse(String batchStr) throws RioDBPluginException;

}
