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

import java.util.Scanner;

/* 
 * The main class (and all contents of package co.riodb.plugin.test are not exported with the plugin.
 * They are just for helping test the plugin. This program loads the plugin and tries it out with 
 * user input.  
 */
public class Main {

	private static Scanner scanner = new Scanner(System.in);

	public static void main(String[] args) {

		boolean quit = false;
		while (!quit) {
			// Display the menu
			System.out.println("\nPick a plugin to test:");
			System.out.println("I. Input");
			System.out.println("O. Output");
			System.out.println("P. Parser");
			System.out.println("F. Formatter");
			System.out.println("Q. Quit");
			System.out.println("-----------------");
			String currentChoice = "";

			if (scanner.hasNextLine()) {
				currentChoice = scanner.nextLine().trim().toUpperCase();

				if (currentChoice == null) {
					System.out.println("Invalid option. Please try again.");
				} else if (currentChoice.equals("I")) {
					InputTester.prompt(scanner);
				} else if (currentChoice.equals("O")) {
					OutputTester.prompt(scanner);
				} else if (currentChoice.equals("P")) {
					ParserTester.prompt(scanner);
				} else if (currentChoice.equals("F")) {
					FormatterTester.prompt(scanner);
					;
				} else if (currentChoice.equals("Q")) {
					quit = true;
				} else {
					System.out.println("Invalid option. Please try again.");
				}
			}
		}
		System.out.println("Good bye!");
		scanner.close(); // Close the scanner when the main loop terminates

	}
}
