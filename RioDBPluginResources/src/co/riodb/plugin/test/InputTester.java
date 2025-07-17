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
import co.riodb.plugin.resources.RioDBInputPlugin;
import co.riodb.plugin.resources.RioDBPluginException;

import java.util.concurrent.atomic.AtomicBoolean; // To safely signal between threads

public class InputTester {

	private static String prompt = "Enter the entire INPUT clause as you would in a RioDB statement, but in a single line. For example:"
			+ "\nINPUT myinput (param1 'abc' param2 0 param3_exists)"
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
				} else if (command.startsWith("input ")) {

					command = command.substring(6);
					String inputPluginName = null;
					String inputParameters = null;
					if (command.contains("(")) {
						inputPluginName = command.substring(0, command.indexOf(" ")).trim().toUpperCase();
						inputParameters = command.substring(inputPluginName.length() + 1).trim();
						if (inputParameters.startsWith("(") && inputParameters.endsWith(")")) {
							inputParameters = inputParameters.substring(1, inputParameters.length() - 1).trim();
						}
						System.out.println("input: " + inputPluginName);
						System.out.println("params: " + inputParameters);

					} else {
						inputPluginName = command.trim().toUpperCase();
					}
					quit = loadPlugin(inputPluginName, inputParameters, scanner);
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
	public static boolean loadPlugin(String inputPluginName, String inputParameters, Scanner scanner) {

		try {

			String pluginClassPath = "co.riodb.plugin.input." + inputPluginName;

			// NEW METHOD
			// 1. Get the Class object for the plugin
			Class<RioDBInputPlugin> pClass = (Class<RioDBInputPlugin>) Class.forName(pluginClassPath);

			// 2. IMPORTANT: Specify constructor
			java.lang.reflect.Constructor<RioDBInputPlugin> pluginConstructor = pClass
					.getDeclaredConstructor(LoggingService.class);

			// 3. Call newInstance() on the *obtained constructor*, passing the
			// Logger
			final Logger logger = new Logger();

			RioDBInputPlugin plugin = pluginConstructor.newInstance(logger);

			try {
				plugin.init(inputParameters);
				Thread.sleep(1000);
				return runPlugin(plugin, scanner);

			} catch (RioDBPluginException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		} catch (ClassNotFoundException e) {
			// Existance already checked a few lines above.
			System.out.println("Input class " + inputPluginName + " has not been installed.");
		} catch (InstantiationException e) {
			System.out.println("InstantiationException for plugin '" + inputPluginName + "'");
		} catch (IllegalAccessException e) {
			System.out.println("IllegalAccessException for plugin '" + inputPluginName + "'");
		} catch (IllegalArgumentException e) {
			System.out.println("IllegalArgumentException for plugin '" + inputPluginName + "'");
		} catch (InvocationTargetException e) {
			System.out.println("InvocationTargetException for plugin '" + inputPluginName + "'");
		} catch (NoSuchMethodException e) {
			System.out.println("NoSuchMethodException for plugin '" + inputPluginName + "'");
		} catch (SecurityException e) {
			System.out.println("SecurityException for plugin '" + inputPluginName + "'");
		}
		return false;
	}

	private static boolean runPlugin(RioDBInputPlugin inputPlugin, Scanner scanner) {
		AtomicBoolean stopPlugin = new AtomicBoolean(false);
		System.out.println("Starting plugin async. Press ENTER to stop it.");

		try {

			inputPlugin.start();

			Thread pluginThread = new Thread(() -> {
				try {
					Thread.sleep(1000); // Wait for 1 second (1000 milliseconds)
					while (!stopPlugin.get()) {
						String msg = inputPlugin.getNextInputMessage();
						System.out.println(msg);
					}

				} catch (InterruptedException e) {
					System.out.println("plugin interrupted.");
				} catch (RioDBPluginException e) {
					e.printStackTrace();
				} finally {
					System.out.println("Plugin stopped.");
				}
			});

			// ----------------- Thread 2: The input listener -----------------
			Thread inputThread = new Thread(() -> {
				scanner.nextLine(); // This blocks until user presses ENTER
				stopPlugin.set(true); // Signal the printer thread to stop
			});

			// Start both threads
			pluginThread.start();
			inputThread.start();

			// Optional: Wait for both threads to finish before main exits
			try {
				inputThread.join(); // Wait for the input thread to complete (user presses ENTER)
				pluginThread.interrupt(); // Interrupt the printer thread if it's still sleeping
				pluginThread.join(); // Wait for the printer thread to fully terminate
			} catch (InterruptedException e) {
				System.err.println("Main thread interrupted while waiting for other threads.");
				return false;
			}

			inputPlugin.stop();

		} catch (RioDBPluginException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

}
