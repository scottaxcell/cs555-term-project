package cs555.project;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class HelloWorld {

    public static void main(String[] args) {
        System.out.println("Hello, World!");
        new HelloWorld().run();
    }

    private void run() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Hello World");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("/s/chopin/a/grad/sgaxcell/cs555-term-project/data/movies_metadata.csv");
        List<String[]> arrays = textFile
            .map(string -> string.split(",", -1))
            .collect();

        arrays.stream()
            .forEach(a -> System.out.println(Arrays.toString(a)));
    }
}
