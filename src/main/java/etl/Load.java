package etl;


import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import static data.Constants.*;

import java.util.List;

/**
 * Load class to write the tables to csv files
 */
public class Load {
    private static final Logger logger = Logger.getLogger(Load.class);
    private static String outputPath = "src/main/resources/data/output/";

    public static void loadAsCsv(String outputPath, Dataset<Row> df) {
        try {
            df
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .option("header", "true")
                .save(outputPath);
            logger.info("File saved successfully to: " + outputPath);
        } catch (Exception e) {
            logger.error("Error writing file: " + outputPath, e);
        }
    }

    public static void writeTables(List<Dataset<Row>> tables) {
        Load.loadAsCsv(outputPath + TABLE_0, tables.get(0));
        Load.loadAsCsv(outputPath + TABLE_1, tables.get(1));
        Load.loadAsCsv(outputPath + TABLE_2, tables.get(2));
        Load.loadAsCsv(outputPath + TABLE_3, tables.get(3));
        Load.loadAsCsv(outputPath + TABLE_4, tables.get(4));
    }
}

