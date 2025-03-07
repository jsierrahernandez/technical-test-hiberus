package etl;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class Extract {

    private static String inputPath = "src/main/resources/data/input/";
    private static final Logger logger = Logger.getLogger(Extract.class);

    public static Dataset<Row> extractCsv(SparkSession spark, String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            return null;
        }

        try {
            return spark.read()
                    .format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .load(filePath);
        } catch (Exception e) {
            return null;
        }
    }

    public static Dataset<Row> extractTable(SparkSession spark) {
        return extractCsv(spark, inputPath+"bank.csv");
    }


}
