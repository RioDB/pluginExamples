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

import java.lang.reflect.InvocationTargetException;
import java.util.Scanner;
import co.riodb.plugin.resources.LoggingService;
import co.riodb.plugin.resources.RioDBFormatterPlugin;
import co.riodb.plugin.resources.RioDBPluginException;

public class FormatterTester {

	private static String prompt = "Enter the entire FORMATTER clause as you would in a RioDB statement, but in a single line. For example:"
			+ "\nFORMATTER delimited (delimiter ',')" + "\nOr to return to main menu, just press ENTER.";

	public static void prompt(Scanner scanner) {
		System.out.println(prompt);
		boolean quit = false;

		while (!quit) {
			if (scanner.hasNextLine()) {
				String command = scanner.nextLine().trim();
				command = TestUtils.lowerCaseEscapingQuoted(command);

				command = TestUtils.formatCommand(command);

				if (command == null || command.equals("")) {
					quit = true;
				} else if (command.startsWith("formatter ")) {

					command = command.substring(10);
					String formatterPluginName = null;
					String formatterParameters = null;
					if (command.contains("(")) {
						formatterPluginName = command.substring(0, command.indexOf(" ")).trim().toUpperCase();
						formatterParameters = command.substring(formatterPluginName.length() + 1).trim();
						if (formatterParameters.startsWith("(") && formatterParameters.endsWith(")")) {
							formatterParameters = formatterParameters.substring(1, formatterParameters.length() - 1)
									.trim();
						}
						System.out.println("formatter: " + formatterPluginName);
						System.out.println("params: " + formatterParameters);

					} else {
						formatterPluginName = command.trim().toUpperCase();
					}
					quit = loadPlugin(formatterPluginName, formatterParameters, scanner);
					if (!quit) {
						System.out.println(prompt);
					}

				} else {
					System.out.println(prompt);
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static boolean loadPlugin(String formatterPluginName, String formatterParameters, Scanner scanner) {

		try {

			String pluginClassPath = "co.riodb.plugin.formatter." + formatterPluginName;

			String fieldHeaders[] = TestUtils.stringSplitter(scanner,
					"Enter the query column headers separated by comma, like name,age,status\n", ",");

			// NEW METHOD
			// 1. Get the Class object for the plugin
			Class<RioDBFormatterPlugin> pClass = (Class<RioDBFormatterPlugin>) Class.forName(pluginClassPath);

			// 2. IMPORTANT: Specify constructor
			java.lang.reflect.Constructor<RioDBFormatterPlugin> pluginConstructor = pClass
					.getDeclaredConstructor(LoggingService.class);

			// 3. Call newInstance() on the *obtained constructor*, passing the
			// Logger
			final Logger logger = new Logger();

			RioDBFormatterPlugin plugin = pluginConstructor.newInstance(logger);

			try {
				plugin.init(formatterParameters, fieldHeaders);
				Thread.sleep(1000);
				return runPlugin(plugin, scanner, fieldHeaders);

			} catch (RioDBPluginException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		} catch (ClassNotFoundException e) {
			// Existance already checked a few lines above.
			System.out.println("Formatter class " + formatterPluginName + " has not been installed.");
		} catch (InstantiationException e) {
			System.out.println("InstantiationException for plugin '" + formatterPluginName + "'");
		} catch (IllegalAccessException e) {
			System.out.println("IllegalAccessException for plugin '" + formatterPluginName + "'");
		} catch (IllegalArgumentException e) {
			System.out.println("IllegalArgumentException for plugin '" + formatterPluginName + "'");
		} catch (InvocationTargetException e) {
			System.out.println("InvocationTargetException for plugin '" + formatterPluginName + "'");
		} catch (NoSuchMethodException e) {
			System.out.println("NoSuchMethodException for plugin '" + formatterPluginName + "'");
		} catch (SecurityException e) {
			System.out.println("SecurityException for plugin '" + formatterPluginName + "'");
		}
		return false;
	}

	private static boolean runPlugin(RioDBFormatterPlugin formatterPlugin, Scanner scanner, String headers[]) {

		String fields[] = new String[headers.length];
		scanner.nextLine();

		while (true) {

			for (int i = 0; i < fields.length; i++) {
				System.out.println("\nEnter value for " + headers[i].trim());
				System.out.print(":");
				String command = scanner.nextLine();
				if (command == null) {
					command = "";
				}
				fields[i] = command;
			}

			try {
				String response = formatterPlugin.format(fields);
				System.out.println("Formatted response:\n");
				System.out.println(response);

			} catch (RioDBPluginException e) {
				System.out.println("Error formating data: " + e.getMessage());
				e.printStackTrace();
				return false;
			}

			System.out.println("\nEnter Q to quit, or ENTER to continue");
			String response = scanner.nextLine();
			if (response != null && response.toLowerCase().equals("q")) {
				return true;
			}

		}

	}

}
