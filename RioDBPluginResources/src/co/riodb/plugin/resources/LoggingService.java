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

// interface for a class that logs
public interface LoggingService {
	void logDebug(String source, String message);

	void logError(String source, String message);

	void logFatal(String source, String message);

	void logInfo(String source, String message);

	void logTrace(String source, String message);

	void logWarn(String source, String message);
}
