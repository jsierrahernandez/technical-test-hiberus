package etl;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.ArrayList;
import java.util.List;

import static data.Constants.*;
import static org.apache.spark.sql.functions.col;

public class Transform {
    private static final Logger logger = Logger.getLogger(Transform.class);

    public static Dataset<Row> calculateAgeRange(Dataset<Row> df) {
        return df.withColumn(AGE_RANGE,
                functions.when(functions.col(AGE).between(10, 19), "10-19")
                        .when(functions.col(AGE).between(20, 29), "20-29")
                        .when(functions.col(AGE).between(30, 39), "30-39")
                        .when(functions.col(AGE).between(40, 49), "40-49")
                        .when(functions.col(AGE).between(50, 59), "50-59")
                        .when(functions.col(AGE).between(60, 69), "60-69")
                        .when(functions.col(AGE).between(70, 79), "70-79")
                        .when(functions.col(AGE).between(80, 89), "80-89")
                        .when(functions.col(AGE).between(90, 99), "90-99")
                        .otherwise("100-109")
        );
    }

    public static Dataset<Row> calculateRangeWhereMostContractLoan(Dataset<Row> df) {
        Dataset<Row> dfWithLoan = df
                .filter(col(LOAN).equalTo(YES))
                .select(AGE);

        Dataset<Row> dfWithAgeRange =  calculateAgeRange(dfWithLoan);

        Dataset<Row> dfRangeWhereMostContractLoan = dfWithAgeRange
                .groupBy(AGE_RANGE)
                .agg(functions.count("*").alias(COUNT))
                .orderBy(functions.desc(COUNT));

        return dfRangeWhereMostContractLoan;
    }

    public static Dataset<Row> calculateAgeRangeAndMaritalStatusWithMoreMoney(Dataset<Row> df) {
        Dataset<Row> initialData = df
                .select(AGE, MARITAL, BALANCE);

        Dataset<Row> dfWithAgeRange = calculateAgeRange(initialData);

        Dataset<Row> dfWithAgeRangeAndMaritalStatus = dfWithAgeRange
                .groupBy(AGE_RANGE, MARITAL)
                .agg(functions.sum(BALANCE).alias(BALANCE))
                .orderBy(functions.desc(BALANCE));

        return dfWithAgeRangeAndMaritalStatus;
    }

    public static Dataset<Row> calculateMostCommonWayOfContractingClientsByRange(Dataset<Row> df, int age1, int age2) {
        Dataset<Row> mostCommonWayOfContractingClientsByRange = df
                .select(AGE, CONTACT)
                .filter(col(AGE).between(age1, age2))
                .groupBy(CONTACT)
                .agg(functions.count("*").alias(TOTAL_EMPLOYES));

        return mostCommonWayOfContractingClientsByRange;
    }

    public static Dataset<Row> calculateAverageMaximumAndMinimumBalanceByCampaignAndMaterialStatusAndProfession(Dataset<Row> df) {
        Dataset<Row> averageMaximumAndMinimumBalanceByCampaignAndMaterialStatusAndProfession = df
                .groupBy(CAMPAIGN, MARITAL, JOB)
                .agg(
                        functions.avg(BALANCE).alias(AVG_BALANCE),
                        functions.max(BALANCE).alias(MAX_BALANCE),
                        functions.min(BALANCE).alias(MIN_BALANCE)
                )
                .orderBy(CAMPAIGN, MARITAL, JOB);

        return averageMaximumAndMinimumBalanceByCampaignAndMaterialStatusAndProfession;
    }

    public static Dataset<Row> calculateMostCommonTypeOfJob(Dataset<Row> df) {
        Dataset<Row> filteredData = df
                .filter(col(MARITAL).equalTo(MARRIED)
                .and(col(HOUSING).equalTo(YES))
                .and(col(BALANCE).gt(1200))
                .and(col(CAMPAIGN).equalTo(3)))
                .select(JOB);

        Dataset<Row> mostCommonTypeOfJob = filteredData
                .groupBy(JOB)
                .agg(functions.count("*").alias(TOTAL_EMPLOYES))
                .orderBy(functions.desc(TOTAL_EMPLOYES));

        return mostCommonTypeOfJob;
    }

    public static List<Dataset<Row>> transformData(Dataset<Row> inputData) {
        List<Dataset<Row>> dfsAfterTransformationsList = new ArrayList<>();

        logger.info("What is the age range, which contract the most loans?");
        Dataset<Row> ageRangeWhereMostContractLoan = calculateRangeWhereMostContractLoan(inputData);
        ageRangeWhereMostContractLoan.show(1, false);
        dfsAfterTransformationsList.add(ageRangeWhereMostContractLoan.limit(1));

        logger.info("What is the age range and marital status that has more money in the accounts?");
        Dataset<Row> ageRangeAndMaritalStatusWithMoreMoney = calculateAgeRangeAndMaritalStatusWithMoreMoney(inputData);
        ageRangeAndMaritalStatusWithMoreMoney.show(1, false);
        dfsAfterTransformationsList.add(ageRangeAndMaritalStatusWithMoreMoney.limit(1));

        logger.info("What is the most common way of contacting clients, between 25-35 years old?");
        Dataset<Row> mostCommonWayOfContractingClients25To35Years = calculateMostCommonWayOfContractingClientsByRange(inputData, 25, 35);
        mostCommonWayOfContractingClients25To35Years.show(1, false);
        dfsAfterTransformationsList.add(mostCommonWayOfContractingClients25To35Years.limit(1));

        logger.info("What is the average, maximum and minimum balance for each type of campaign, taking into account their marital status and profession?");
        Dataset<Row> averageMaximumAndMinimumBalanceByCampaignAndMaterialStatusAndProfession =
                calculateAverageMaximumAndMinimumBalanceByCampaignAndMaterialStatusAndProfession(inputData);
        long total = averageMaximumAndMinimumBalanceByCampaignAndMaterialStatusAndProfession.count();
        averageMaximumAndMinimumBalanceByCampaignAndMaterialStatusAndProfession.show((int) total, false);;
        dfsAfterTransformationsList.add(averageMaximumAndMinimumBalanceByCampaignAndMaterialStatusAndProfession);

        logger.info("What is the most common type of job, among those who are married (job=married), who have their own house (housing=yes), and who have more than 1,200€ in the account and who are from campaign 3?");
        Dataset<Row> mostCommonTypeOfJob = calculateMostCommonTypeOfJob(inputData);
        long total2 = mostCommonTypeOfJob.count();
        mostCommonTypeOfJob.show((int) total2, false);
        dfsAfterTransformationsList.add(mostCommonTypeOfJob);

        return dfsAfterTransformationsList;
    }
}
