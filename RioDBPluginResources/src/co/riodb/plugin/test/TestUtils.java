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
package co.riodb.plugin.test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.InputMismatchException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TestUtils {

	public static int getValidPositiveInteger(Scanner scanner, String promptMessage) {
		int value = -1; // Initialize with an invalid value
		boolean validInput = false;

		while (!validInput) {
			System.out.print(promptMessage); // Display the prompt

			try {
				if (scanner.hasNextInt()) { // Check if the next token is an integer
					value = scanner.nextInt(); // Read the integer

					if (value > 0) { // Check if it's positive
						validInput = true; // Input is valid, exit loop
					} else {
						System.out.println("Invalid input: Please enter a positive number.");
					}
				} else {
					// User entered something that's not an integer
					System.out.println("Invalid input: Please enter a whole number.");
				}
			} catch (InputMismatchException e) {
				// This catch block is generally not hit if hasNextInt() is used correctly,
				// but it's good practice for robustness if you were to use nextInt() directly
				// without hasNextInt() check.
				System.out.println("Invalid input: That was not a number. Please try again.");
			} finally {
				// Consume the rest of the line (including the newline character)
				// This is crucial to prevent the next nextInt() call from trying to
				// parse the remainder of the current line if it was invalid.
				scanner.nextLine();
			}
		}
		return value;
	}

	public static String[] stringSplitter(Scanner scanner, String promptMessage, String delimiter) {
		String value = ""; // Initialize with an invalid value
		boolean validInput = false;

		while (!validInput) {
			System.out.print(promptMessage); // Display the prompt

			value = scanner.next(); // Read the integer
			if (value != null && value.length() > 0) { // Check if it's positive
				String values[] = value.split(delimiter);
				return values;
			} else {
				System.out.println("Invalid input: Please enter fields separated by '" + delimiter + "'");
			}
		}
		return null;
	}

	public static final String formatCommand(String command) {

		command = r_removeComments(command);
		command = r_encodeQuotedText(command);
		command = command.replace("(", " (");
		while (command.contains("  ")) {
			command = command.replace("  ", " ");
		}
		command = r_decodeQuotedText(command);

		if (command.endsWith(";")) {
			command = command.substring(0, command.length() - 1);
		}

		return command.trim();
	}

	public static final String r_removeComments(String r_stmt1) {
		String r_newStmt1 = r_stmt1;
		if (r_newStmt1 != null) {
			boolean r_insideComment = false;
			boolean r_insideStatic = false;
			for (int r_i = 0; r_i < r_newStmt1.length(); r_i++) {
				if (r_newStmt1.charAt(r_i) == '\'' && !r_insideComment) {
					if (r_insideStatic)
						r_insideStatic = false;
					else
						r_insideStatic = true;
				} else if (r_newStmt1.charAt(r_i) == '#' && !r_insideStatic) {
					r_insideComment = true;
					r_newStmt1 = r_newStmt1.substring(0, r_i) + " " + r_newStmt1.substring(r_i + 1);
				} else if (r_newStmt1.charAt(r_i) == '\n' || r_newStmt1.charAt(r_i) == '\r') {
					r_insideComment = false;
				} else if (r_insideComment) {
					// remove the current char
					r_newStmt1 = r_newStmt1.substring(0, r_i) + " " + r_newStmt1.substring(r_i + 1);
				}
			}
		}
		return r_newStmt1;
	}

	public static final String r_decodeQuotedText(String r_string1) {

		// System.out.println("decodeQuotedText:"+s);

		if (r_string1 == null) {
			return null;
		}

		boolean r_inQuotes = false;
		ArrayList<Integer> r_quoteBeginList = new ArrayList<Integer>();
		ArrayList<Integer> r_quoteEndList = new ArrayList<Integer>();
		for (int r_i = 0; r_i < r_string1.length(); r_i++) {
			if (r_string1.charAt(r_i) == '\'') {
				if (r_inQuotes) {
					r_inQuotes = false;
					r_quoteEndList.add(r_i);
				} else {
					r_inQuotes = true;
					r_quoteBeginList.add(r_i);
				}
			}
		}

		String r_returnStr = r_string1;

		for (int r_i = 0; r_i < r_quoteEndList.size(); r_i++) {

			if (r_i < r_quoteEndList.size()) {
				String r_tempStr = r_string1.substring(r_quoteBeginList.get(r_i) + 1, r_quoteEndList.get(r_i));
				String r_originalStr = r_tempStr;
				r_tempStr = r_tempStr.replace("$", "=");
				r_tempStr = r_tempStr.replace(" ", "");

				// confirm if the string is indeed base64 encoded:
				Pattern r_pattern = Pattern.compile("^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)?$");
				Matcher r_matcher = r_pattern.matcher(r_tempStr);
				if (r_matcher.find()) {
					// it is base64 encoded.
					byte[] r_decodedBytes = Base64.getDecoder().decode(r_tempStr);
					String r_decodedStr = new String(r_decodedBytes);
					// t = "'" + t.replace("=", "$") + "'";
					r_originalStr = "'" + r_originalStr + "'";
					r_decodedStr = "'" + r_decodedStr + "'";
					r_returnStr = r_returnStr.replace(r_originalStr, r_decodedStr);
				}
			}
		}

		// System.out.println("Decoded: "+ r);

		return r_returnStr;

	}

	// function to decode base64 text in quotes only. Substituting single quotes for
	// double quotes.
	public static final String r_decodeQuotedTextToDoubleQuoted(String r_string2) {

		// System.out.println("decodeQuotedTextToDoubleQuoted: "+ s);

		if (r_string2 == null) {
			return null;
		}

		boolean r_inQuote = false;
		ArrayList<Integer> r_quoteBeginList2 = new ArrayList<Integer>();
		ArrayList<Integer> r_quoteEndList2 = new ArrayList<Integer>();
		for (int r_i = 0; r_i < r_string2.length(); r_i++) {
			if (r_string2.charAt(r_i) == '\'') {
				if (r_inQuote) {
					r_inQuote = false;
					r_quoteEndList2.add(r_i);
				} else {
					r_inQuote = true;
					r_quoteBeginList2.add(r_i);
				}
			}
		}

		String r_returnStr2 = r_string2;
		for (int r_i = 0; r_i < r_quoteEndList2.size(); r_i++) {

			if (r_i < r_quoteEndList2.size()) {
				String r_tempStr2 = r_string2.substring(r_quoteBeginList2.get(r_i) + 1, r_quoteEndList2.get(r_i));
				String r_originalStr2 = r_tempStr2;
				r_tempStr2 = r_tempStr2.replace("$", "=");
				r_tempStr2 = r_tempStr2.replace(" ", "");

				// confirm if the string is indeed base64 encoded:
				Pattern r_pattern2 = Pattern.compile("^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)?$");
				Matcher r_matcher2 = r_pattern2.matcher(r_tempStr2);
				if (r_matcher2.find()) {
					// it is base64 encoded.

					byte[] r_decodedBytes2 = Base64.getDecoder().decode(r_tempStr2);
					String r_decodedStr2 = new String(r_decodedBytes2);
					r_decodedStr2.replace("''", "'");

					// t = "'" + t.replace("=", "$") + "'";
					// d = "\"" + d + "\"";
					// r = r.replace(t, d);

					r_originalStr2 = "'" + r_originalStr2 + "'";
					r_decodedStr2 = "\"" + r_decodedStr2 + "\"";
					r_returnStr2 = r_returnStr2.replace(r_originalStr2, r_decodedStr2);

				}
			}
		}

		// System.out.println("Decoded: "+ r);

		return r_returnStr2;

	}

	// function to decode base64
	public static final String r_decodeText(String r_string3) {

		// System.out.println("decodeText: "+ s);
		try {
			byte[] r_decodedBytes3 = Base64.getDecoder().decode(r_string3.replace("$", "=").replace(" ", ""));
			return new String(r_decodedBytes3);
		} catch (IllegalArgumentException r_iae) {
			// if s is invalid Base64, return original s
			return r_string3;
		}
	}

	// Function to encode any quoted text into base64
	public static final String r_encodeQuotedText(String r_string4) {

		// System.out.println("Encoding: "+ s);
		if (r_string4 == null) {
			return null;
		}

		boolean r_inQuote4 = false;
		ArrayList<Integer> r_quoteBeginList4 = new ArrayList<Integer>();
		ArrayList<Integer> r_quoteEndList4 = new ArrayList<Integer>();
		for (int r_i = 0; r_i < r_string4.length(); r_i++) {
			if (r_string4.charAt(r_i) == '\'') {
				if (r_inQuote4) {
					if (r_i < r_string4.length() - 1 && r_string4.charAt(r_i + 1) == '\'') {
						r_i++;
					} else {
						r_inQuote4 = false;
						r_quoteEndList4.add(r_i);

					}
				} else {
					r_inQuote4 = true;
					r_quoteBeginList4.add(r_i);
				}
			}
		}

		String r_returnStr4 = r_string4;
		for (int r_i = 0; r_i < r_quoteEndList4.size(); r_i++) {
			if (r_i < r_quoteEndList4.size()) {
				String r_tempStr4 = r_string4.substring(r_quoteBeginList4.get(r_i) + 1, r_quoteEndList4.get(r_i));
				String r_encodedStr4 = Base64.getEncoder().encodeToString(r_tempStr4.getBytes());
				r_encodedStr4 = r_encodedStr4.replace("=", "$");
				r_tempStr4 = "'" + r_tempStr4 + "'";
				r_encodedStr4 = "'" + r_encodedStr4 + "'";
				r_returnStr4 = r_returnStr4.replace(r_tempStr4, r_encodedStr4);
				// System.out.println(t + " -> " + e);
			}
		}

		// System.out.println("Encoded: "+ r);

		return r_returnStr4;

	}

	public static String lowerCaseEscapingQuoted(String r_stmt20) {
		if (r_stmt20 == null) {
			return null;
		}

		StringBuilder r_sb = new StringBuilder();

		// in loop, characters that are NOT within quotes are changed to
		// lowercase.
		boolean r_insideQuoteq = false;
		for (int r_i = 0; r_i < r_stmt20.length(); r_i++) {
			if (r_stmt20.charAt(r_i) == '\'') {
				if (r_insideQuoteq) {
					r_insideQuoteq = false;
				} else {
					r_insideQuoteq = true;
				}
				r_sb.append('\'');
			} else if (r_insideQuoteq) {
				r_sb.append(r_stmt20.charAt(r_i));
			} else {
				r_sb.append(Character.toLowerCase(r_stmt20.charAt(r_i)));
			}
		}

		String r_newString = r_sb.toString();

		// correct special words
		r_newString = r_newString.replace("double.nan", "Double.NaN");

		return r_newString;
	}
}
