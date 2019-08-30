package sparkstarter;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.assertj.core.api.Assertions.assertThat;

class SparkStarterJobTest {
    private static SparkSession spark;
    private static JavaSparkContext sc;
    private static SparkStarterJob job;

    @BeforeAll
    static void setup() {
        spark = SparkSession.builder()
                .appName("Spark Starter")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        sc = new JavaSparkContext(spark.sparkContext());
        job = new SparkStarterJob(spark);
    }

    @AfterAll
    static void teardown() {
        spark.close();
    }

    @Test
    void total() {
        assertThat(job.sum()).isEqualTo(285);
    }

    @Test
    void customers() {
        final Dataset<Customer> customersDS = job.customers(5);

        customersDS.write()
                .mode(SaveMode.Overwrite)
                .saveAsTable("customers");

        final List<Customer> customers = customersDS.collectAsList();


        final List<Customer> expectedCustomers = IntStream.range(1, 5)
                .mapToObj(id -> new Customer(id, String.format("Name %d", id)))
                .collect(Collectors.toList());

        assertThat(customers).isEqualTo(expectedCustomers);
    }

    @Test
    void readCsvAsDataset() throws URISyntaxException {
        final Path petCsvPath = Paths.get(getClass().getResource("/pets.csv").toURI());

        final StructType petSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true)
        });

        final Encoder<Pet> petsEncoder = Encoders.bean(Pet.class);

        final Dataset<Pet> petsDS = spark.read()
                .option("header", true)
                .schema(petSchema)
                .csv(petCsvPath.toString())
                .as(petsEncoder);

        final List<Pet> expectedPets = Arrays.asList(
                new Pet(1, "Rex"),
                new Pet(2, "Mistigri"),
                new Pet(3, "Randolph")
        );

        final List<Pet> pets = petsDS.collectAsList();
        assertThat(pets).isEqualTo(expectedPets);
    }

    @Test
    void groupedSquareSums() {
        final Dataset<Row> numbersDF = spark.range(1, 10)
                .select(col("id").as("number"));

        final Dataset<Row> groupedSquareSumsDF = numbersDF
                .select(
                        group(col("number")).as("group"),
                        square(col("number")).as("square")
                )
                .groupBy("group")
                .agg(sum(col("square")).as("sum_of_squares"))
                .sort("group");

        groupedSquareSumsDF.show();
    }

    private Column group(final Column i) {
        return i.mod(10).plus(1);
    }

    private Column square(final Column i) {
        return i.multiply(i);
    }
}
