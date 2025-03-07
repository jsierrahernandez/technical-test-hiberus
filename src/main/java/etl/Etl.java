package etl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * This class contains the ETL process.
 */
public class Etl {
    private static final Logger logger = Logger.getLogger(Etl.class);

    public static void run() {
        logger.getLogger("org").setLevel(Level.WARN);
        logger.getLogger("akka").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("MyEtl")
                .master("local[*]")
                .getOrCreate();

        logger.info("Extract Data...");
        Dataset<Row> inputData = Extract.extractTable(spark);
        if (inputData == null) {
            throw new RuntimeException("No input found. Please check the path and try again");
        } else {
            logger.info("Data loaded successfully.");
        }

        logger.info("Transform Data...");
        List<Dataset<Row>> transformedData = Transform.transformData(inputData);
        if (transformedData.size() != 5) {
            throw new RuntimeException("No all output tables were created");
        } else {
            logger.info("Data Transformed successfully.");
        }

        logger.info("Load data...");
        try {
            Load.writeTables(transformedData);
        } catch (Exception e) {
            throw new RuntimeException("Error loading data: " + e.getMessage());
        }


        spark.stop();
        logger.info("ETL finished.");
    }
}
