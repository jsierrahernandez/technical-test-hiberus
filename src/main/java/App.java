import etl.Etl;
import etl.Extract;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import sql.Client;

import java.util.Scanner;

/**
 * Main class of the application. It allows the user to choose between running a query with Spark Sql or running the ETL.
 * The user can also exit the application.
 */
public class App {
    private static final Logger logger = Logger.getLogger(App.class);

    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);
        int option = 0; //option 0 is a default value to enter the loop

        while(option == 0 || option == 1 || option == 2 || option == 3) {
            logger.info("Please choose an option (1, 2 or 3). Other values will be ignored.");
            logger.info("1) Run your own query with Spark Sql");
            logger.info("2) Run ETL");
            logger.info("3) Exit");

            try {
                option = Integer.parseInt(scanner.nextLine());
            } catch (NumberFormatException e) {
                option = 0;
            }

            if (option == 1) {
                logger.info("Introduce your query:");
                String query = scanner.nextLine();

                try {
                    Client.run(query);
                } catch (Exception e) {
                    logger.info("There was an error running spark sql client: " + e.getMessage());
                }

            } else if (option == 2) {
                logger.info("Running ETL...");
                try {
                    Etl.run();
                } catch (Exception e) {
                    logger.info("There was an error running ETL: " + e.getMessage());
                }

            } else if (option == 3) {
                logger.info("Application finished.");
                break;
            }
            else {
                logger.info("Invalid input. Please enter 1, 2 or 3.");
                option = 0;
            }

        }
        scanner.close();
        }

}



