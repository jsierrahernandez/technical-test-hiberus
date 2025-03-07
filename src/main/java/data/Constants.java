package data;

/**
 * Class containing the constants used in the project.
 * These constants represent different columns and values in data processing.
 */
public class Constants {

    //Prevent instantiation
    private Constants() {
        throw new UnsupportedOperationException("Cannot instantiate Constants class");
    }

    //Column constants
    public static final String LOAN = "loan";
    public static final String AGE = "age";
    public static final String AGE_RANGE = "age_range";
    public static final String COUNT = "count";
    public static final String MARITAL = "marital";
    public static final String BALANCE = "balance";
    public static final String CONTACT = "contact";
    public static final String TOTAL_EMPLOYES = "total_employees";
    public static final String CAMPAIGN = "campaign";
    public static final String JOB = "job";
    public static final String MARRIED = "married";
    public static final String HOUSING = "housing";
    public static final String YES = "yes";
    public static final String AVG_BALANCE = "avg_balance";
    public static final String MAX_BALANCE = "max_balance";
    public static final String MIN_BALANCE = "min_balance";

    //Table constants
    public static final String TABLE_0 = "age_range_where_most_contract_loan";
    public static final String TABLE_1 = "age_range_and_marital_status_with_more_Money";
    public static final String TABLE_2 = "most_common_way_of_contracting_clients_25_to_35_years";
    public static final String TABLE_3 = "average_maximum_and_minimum_balance_by_campaign_and_material_status_and_profession";
    public static final String TABLE_4 = "most_common_type_of_job";

}
