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
import co.riodb.plugin.resources.RioDBOutputPlugin;
import co.riodb.plugin.resources.RioDBPluginException;

public class OutputTester {

	private static String prompt = "Enter the entire OUTPUT clause as you would in a RioDB statement, but in a single line. For example:"
			+ "\nOUTPUT http (url 'https://localhost:443/something')"
			+ "\nOr to return to main menu, just press ENTER.";

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
				} else if (command.startsWith("output ")) {

					command = command.substring(7);
					String outputPluginName = null;
					String outputParameters = null;
					if (command.contains("(")) {
						outputPluginName = command.substring(0, command.indexOf(" ")).trim().toUpperCase();
						outputParameters = command.substring(outputPluginName.length() + 1).trim();
						if (outputParameters.startsWith("(") && outputParameters.endsWith(")")) {
							outputParameters = outputParameters.substring(1, outputParameters.length() - 1).trim();
						}
						System.out.println("output: " + outputPluginName);
						System.out.println("params: " + outputParameters);

					} else {
						outputPluginName = command.trim().toUpperCase();
					}
					quit = loadPlugin(outputPluginName, outputParameters, scanner);
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
	public static boolean loadPlugin(String outputPluginName, String outputParameters, Scanner scanner) {

		try {

			String pluginClassPath = "co.riodb.plugin.output." + outputPluginName;

			// NEW METHOD
			// 1. Get the Class object for the plugin
			Class<RioDBOutputPlugin> pClass = (Class<RioDBOutputPlugin>) Class.forName(pluginClassPath);

			// 2. IMPORTANT: Specify constructor
			java.lang.reflect.Constructor<RioDBOutputPlugin> pluginConstructor = pClass
					.getDeclaredConstructor(LoggingService.class);

			// 3. Call newInstance() on the *obtained constructor*, passing the
			// Logger
			final Logger logger = new Logger();

			RioDBOutputPlugin plugin = pluginConstructor.newInstance(logger);

			try {
				plugin.init(outputParameters);
				Thread.sleep(1000);
				return runPlugin(plugin, scanner);

			} catch (RioDBPluginException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		} catch (ClassNotFoundException e) {
			// Existance already checked a few lines above.
			System.out.println("Output class " + outputPluginName + " has not been installed.");
		} catch (InstantiationException e) {
			System.out.println("InstantiationException for plugin '" + outputPluginName + "'");
		} catch (IllegalAccessException e) {
			System.out.println("IllegalAccessException for plugin '" + outputPluginName + "'");
		} catch (IllegalArgumentException e) {
			System.out.println("IllegalArgumentException for plugin '" + outputPluginName + "'");
		} catch (InvocationTargetException e) {
			System.out.println("InvocationTargetException for plugin '" + outputPluginName + "'");
		} catch (NoSuchMethodException e) {
			System.out.println("NoSuchMethodException for plugin '" + outputPluginName + "'");
		} catch (SecurityException e) {
			System.out.println("SecurityException for plugin '" + outputPluginName + "'");
		}
		return false;
	}

	private static boolean runPlugin(RioDBOutputPlugin outputPlugin, Scanner scanner) {
		try {

			outputPlugin.start();
			Thread.sleep(2000);
			System.out.println("Plugin started.");

			boolean quit = false;

			while (!quit) {

				System.out.println(
						"\nEnter a sample payload all in one line, such as:\n" + "{\"message\":\"Hello World!\"}");
				String payload = scanner.nextLine();

				outputPlugin.sendOutput(payload);
				Thread.sleep(1000);

				System.out.println("\nPayload sent. Enter Q to quit, or ENTER to continue");
				String response = scanner.nextLine();
				if (response != null && response.toLowerCase().equals("q")) {
					quit = true;
				}
			}

			outputPlugin.stop();

		} catch (RioDBPluginException e) {
			System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}

}
