package sql;

import etl.Etl;
import etl.Extract;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Client {
    private static final Logger logger = Logger.getLogger(Client.class);

    public static void run(String query){
        logger.getLogger("org").setLevel(Level.WARN);
        logger.getLogger("akka").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("MySqlQuery")
                .master("local[*]")
                .getOrCreate();

        logger.info("Extract Data...");
        Dataset<Row> inputData = Extract.extractTable(spark);
        if (inputData == null) {
            throw new RuntimeException("No input found. Please check the path and try again");
        } else {
            logger.info("Data loaded successfully.");
        }

        try {
            inputData.createOrReplaceTempView("bank");
            Dataset<Row> result = spark.sql(query);
            long total = result.count();
            result.show((int) total, false);
            logger.info("Query runned successfully.");
        } catch (Exception e) {
            throw new RuntimeException("Error running query: " + e.getMessage());
        }

        spark.stop();
    }
}
