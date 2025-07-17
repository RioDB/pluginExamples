package co.riodb.plugin.test;

import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.Scanner;
import co.riodb.plugin.resources.LoggingService;
import co.riodb.plugin.resources.RioDBParserPlugin;
import co.riodb.plugin.resources.RioDBPluginException;

public class ParserTester {

	private static String prompt = "Enter the entire PARSER clause as you would in a RioDB statement, but in a single line. For example:"
			+ "\nPARSER delimited (delimiter ',')"
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
				} else if (command.startsWith("parser ")) {

					command = command.substring(7);
					String parserPluginName = null;
					String parserParameters = null;
					if (command.contains("(")) {
						parserPluginName = command.substring(0, command.indexOf(" ")).trim().toUpperCase();
						parserParameters = command.substring(parserPluginName.length() + 1).trim();
						if (parserParameters.startsWith("(") && parserParameters.endsWith(")")) {
							parserParameters = parserParameters.substring(1, parserParameters.length() - 1).trim();
						}
						System.out.println("parser: " + parserPluginName);
						System.out.println("params: " + parserParameters);

					} else {
						parserPluginName = command.trim().toUpperCase();
					}
					quit = loadPlugin(parserPluginName, parserParameters, scanner);
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
	public static boolean loadPlugin(String parserPluginName, String parserParameters, Scanner scanner) {

		try {

			String pluginClassPath = "co.riodb.plugin.parser." + parserPluginName;


			int fieldCount = TestUtils.getValidPositiveInteger(scanner, "How many fields is the parser expected to fill? (enter a number)\n");
			
			
			
			// NEW METHOD
			// 1. Get the Class object for the plugin
			Class<RioDBParserPlugin> pClass = (Class<RioDBParserPlugin>) Class.forName(pluginClassPath);

			// 2. IMPORTANT: Specify constructor
			java.lang.reflect.Constructor<RioDBParserPlugin> pluginConstructor = pClass
					.getDeclaredConstructor(LoggingService.class);

			// 3. Call newInstance() on the *obtained constructor*, passing the
			// Logger
			final Logger logger = new Logger();

			RioDBParserPlugin plugin = pluginConstructor.newInstance(logger);

			try {
				plugin.init(parserParameters, fieldCount);
				Thread.sleep(1000);
				return runPlugin(plugin, scanner, fieldCount);

			} catch (RioDBPluginException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		} catch (ClassNotFoundException e) {
			// Existance already checked a few lines above.
			System.out.println("Parser class " + parserPluginName + " has not been installed.");
		} catch (InstantiationException e) {
			System.out.println("InstantiationException for plugin '" + parserPluginName + "'");
		} catch (IllegalAccessException e) {
			System.out.println("IllegalAccessException for plugin '" + parserPluginName + "'");
		} catch (IllegalArgumentException e) {
			System.out.println("IllegalArgumentException for plugin '" + parserPluginName + "'");
		} catch (InvocationTargetException e) {
			System.out.println("InvocationTargetException for plugin '" + parserPluginName + "'");
		} catch (NoSuchMethodException e) {
			System.out.println("NoSuchMethodException for plugin '" + parserPluginName + "'");
		} catch (SecurityException e) {
			System.out.println("SecurityException for plugin '" + parserPluginName + "'");
		}
		return false;
	}

	private static boolean runPlugin(RioDBParserPlugin parserPlugin, Scanner scanner, int fieldCount) {
		
		while (true) {
			System.out.println("\nEnter a text string that the parser is supposed to separate into "+fieldCount+" text fields. Or just press ENTER to quit.");

			if (scanner.hasNextLine()) {
				String command = scanner.nextLine();
				if(command == null || command.length() == 0) {
					return true;
				}
						
				try {
					LinkedList<String[]> response = parserPlugin.parse(command);
					
					if(response != null) {
					
					int listIndex = 0;
			        for (String[] stringArray : response) {
			            
			            // Inner loop: Iterate through each row
			            if (stringArray != null) { // IMPORTANT: Check if the array itself is not null
			                if (stringArray.length == 0) {
			                    System.out.println("Row "+ listIndex + " is empty.");
			                } else {
			                    int arrayIndex = 0;
			                    for (String str : stringArray) {
			                        System.out.println("Field[" + arrayIndex + "]: \t" + str);
			                        arrayIndex++;
			                    }
			                }
			            } else {
			                System.out.println("  (This entry in the LinkedList is null)");
			            }
			            listIndex++;
			        }
					
					} else {
						System.out.println("\nParser returned NULL. Try again...");
					}
				} catch (RioDBPluginException e) {
					System.out.println("ERROR parsing:\n"+e.getMessage());
				}
				
			}
		}
	}

}
