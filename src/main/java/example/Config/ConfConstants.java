package example.Config;

public class ConfConstants {

    final public static String appName = "spark-to-Hive-job";
    final public static String master = "local[*]";
    final public static String source = "hive";
    final public static String topic = "logging_Topic";
    final public static String autoOffset = "latest";
    final public static String enableAutoOffset = "true";
    final public static String bootStrapServer = "localhost:9092";
    final public static String warehouseloc = "spark-to-Hive-job";
    final public static int lastIndex = 12;
    final public static String destHiveTab = "logging_detail";
    final public static String defaultStrValue = "NA";
    final public static int defaultIntValue = -1;
    final public static String[] stringColList = {"job_execution_id","client_name","client_app_name","tenant","business_category"
            ,"business_subcategory","execution_mode","rule_set_detail","rule_detail","source_detail","target_detail"};
    final public static String[] numColList = {"status"};

}
