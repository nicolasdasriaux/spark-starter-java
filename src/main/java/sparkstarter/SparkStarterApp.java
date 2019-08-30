package sparkstarter;

import org.apache.spark.sql.SparkSession;

public class SparkStarterApp {
    public static void main(final String[] args) {
        final SparkSession spark = SparkSession.builder()
                .appName("Spark Starter")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        final SparkStarterJob job = new SparkStarterJob(spark);
        System.out.println(job.sum());
        spark.close();
    }
}
