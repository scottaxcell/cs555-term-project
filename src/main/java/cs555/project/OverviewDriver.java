package cs555.project;

import cs555.project.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class OverviewDriver {
    public static void main(String[] args) {
        new OverviewDriver().run();
    }

    private void run() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Overview");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("/s/chopin/a/grad/sgaxcell/cs555-term-project/data/movies_metadata.csv");
        Utils.debug("# rows processed: " + (long) textFile
            .map(Utils::splitCommaDelimitedString)
            .filter(MoviesMetadataHelper::isRowValid)
            .collect()
            .size());
    }
}
