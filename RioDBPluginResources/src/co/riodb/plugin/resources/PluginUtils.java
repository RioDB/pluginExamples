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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

// helper class for repetitive procedures
public final class PluginUtils {

	// array of string split by space. Consolidates items that were originally
	// quoted.
	private static String[] consolidateQuotedText(String inputArr[]) {

		for (int i = 0; i < inputArr.length; i++) {
			if (inputArr[i].startsWith("'") && !inputArr[i].endsWith("'") && inputArr[i].length() > 1) {
				for (int j = i + 1; j < inputArr.length; j++) {
					inputArr[i] = inputArr[i] + " " + inputArr[j];
					if (inputArr[j].endsWith("'")) {
						inputArr[j] = "";
						break;
					}
					inputArr[j] = "";
				}
			}
		}

		int emptyCounter = 0;
		for (String eachStr : inputArr) {
			if (eachStr != null && eachStr.equals(""))
				emptyCounter++;
		}

		String newArr[] = new String[inputArr.length - emptyCounter];
		int fieldMarker = 0;

		for (int i = 0; i < (inputArr.length); i++) {
			if (!inputArr[i].equals("")) {
				newArr[fieldMarker++] = inputArr[i];
				if (fieldMarker == newArr.length) {
					break;
				}
			}
		}

		return newArr;
	}

	/*
	 * getParameter
	 *
	 * function to obtain a parameter value from a parameter string
	 */
	public static String getParameter(String paramStr, String key) {

		if (paramStr == null || paramStr.length() < 3 || !paramStr.contains(" ")) {
			return null;
		}

		String params[] = paramStr.split(" ");

		params = consolidateQuotedText(params);

		for (int i = 0; i < params.length; i++) {
			if (key.equals(params[i].toLowerCase()) && i < params.length - 1) {
				String paramValue = params[i + 1];
				if (paramValue.startsWith("'") && paramValue.endsWith("'") && paramValue.length() > 1) {
					paramValue = paramValue.substring(1, paramValue.length() - 1);
				} else if (paramValue.startsWith("'")) {
					paramValue = paramValue.substring(1);
					for (int j = i + 2; j < params.length; j++) {
						if (params[j].endsWith("'")) {
							paramValue = paramValue + " " + params[j].substring(0, params[j].length() - 1);
							break;
						} else {
							paramValue = paramValue + " " + params[j];
						}
					}
				}
				return paramValue;
			}
		}
		return null;
	}

	/*
	 * hasParameter Function to check if an array of words contains a parameter
	 * name.
	 */
	public static boolean hasParameter(String paramStr, String key) {
		if (paramStr == null || paramStr.length() < 3 || !paramStr.contains(" ")) {
			return false;
		}

		String params[] = paramStr.split(" ");

		params = consolidateQuotedText(params);

		for (int i = 0; i < params.length; i++) {
			if (key.equals(params[i].toLowerCase())) {

				return true;

			} else if (params[i].startsWith("'")) {

				if (params[i].length() > 1 && params[i].endsWith("'")) {
					// skip
				} else {
					// if a string is enclosed with single quotes like 'one to three', skip it
					for (int j = i + 1; j < params.length; j++) {
						if (params[j].endsWith("'")) {
							break;
						}
					}
				}
			}
		}
		return false;
	}

	/*
	 * function to test if a string is numeric.
	 */
	public static boolean isNumber(String str) {
		if (str == null) {
			return false;
		}
		try {
			Double.parseDouble(str);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}

	/*
	 * function to test if a string is a positive integer. Useful to validate port
	 * number, etc
	 */
	public static boolean isPositiveInteger(String str) {
		if (str == null)
			return false;
		try {
			if (Integer.parseInt(str) > 0) {
				return true;
			}
			return false;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	/*
	 * inputStreamToString
	 * 
	 * converts input stream to a string
	 * 
	 */
	public String inputStreamToString(InputStream inputStream) throws RioDBPluginException {

		String response = "";
		StringBuilder strBuilder = new StringBuilder();
		Reader reader = new BufferedReader(
				new InputStreamReader(inputStream, Charset.forName(StandardCharsets.UTF_8.name())));
		int i = 0;
		try {
			while ((i = reader.read()) != -1) {
				strBuilder.append((char) i);
			}
			response = strBuilder.toString();
		} catch (IOException e) {
			throw new RioDBPluginException("IOException while converting InputStream to String");
		}

		return response;

	}

}
