package sparkstarter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SparkStarterJob {
    private final SparkSession spark;
    private final JavaSparkContext sc;

    public SparkStarterJob(final SparkSession spark) {
        this.spark = spark;
        this.sc = new JavaSparkContext(spark.sparkContext());
    }

    public int sum() {
        final List<Integer> numbers = IntStream.range(1, 10).boxed().collect(Collectors.toList());
        final JavaRDD<Integer> squares = sc.parallelize(numbers).map(i -> i * i);
        final Integer sum = squares.fold(0, Integer::sum);
        return sum;
    }

    public Dataset<Customer> customers(final int count) {
        final List<Customer> customers = IntStream.range(1, count)
                .mapToObj(id -> new Customer(id, String.format("Name %d", id)))
                .collect(Collectors.toList());

        final Dataset<Customer> customersDS = spark.createDataset(customers, Encoders.bean(Customer.class));
        return customersDS;
    }
}
