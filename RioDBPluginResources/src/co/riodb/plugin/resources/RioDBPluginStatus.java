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

import java.util.concurrent.atomic.AtomicInteger;

public class RioDBPluginStatus {

	// static final list of status
	private static final String[] statusMap = { "STOPPED", "OK", "WARNING", "ERROR", "FATAL" };

	// static final list of status description
	private static final String[] statusDesc = { "STOPPED: Initialized by not running.", "OK: Running.",
			"WARNING: Encoutered invalid data.", "ERROR: Encountered internal or communications error.",
			"FATAL: Plugin is non-operational" };

	// code (array index) of THIS RioDBPluginStatus object
	private AtomicInteger statusCode;

	// constructor. 0-3 are allowed.
	public RioDBPluginStatus() {
		this.statusCode = new AtomicInteger(0);
	}

	// constructor. 0-3 are allowed.
	public RioDBPluginStatus(int status) {
		this.statusCode = new AtomicInteger(status);
	}

	// get status display name
	public String getStatus() {
		return statusMap[statusCode.get()];
	}

	// get status code
	public int getStatusCode() {
		return statusCode.get();
	}

	// get status description
	public String getStatusDesc() {
		return statusDesc[statusCode.get()];
	}

	public void setError() {
		this.statusCode.set(3);
	}

	public void setFatal() {
		this.statusCode.set(4);
	}

	public void setInitialized() {
		this.statusCode.set(0);
	}

	public void setOk() {
		this.statusCode.set(1);
	}

	// setters
	public void setStopped() {
		this.statusCode.set(0);
	}

	public void setWarning() {
		this.statusCode.set(2);
	}

}
