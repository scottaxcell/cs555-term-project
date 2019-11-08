package cs555.project;

import cs555.project.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Objects;

public class BudgetDriver {
    public static void main(String[] args) {
        new BudgetDriver().run();
    }

    private void run() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Budget Analysis");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("/s/chopin/a/grad/sgaxcell/cs555-term-project/data/movies_metadata.csv");

        // find unique budgets; budget is considered valid if it is greater than 0
        List<Integer> budgets = textFile
                .map(Utils::splitCommaDelimitedString)
                .filter(MoviesMetadataHelper::isRowValid)
                .filter(MoviesMetadataHelper::isMovieSuccessfulByVoteAverage)
                .map(MoviesMetadataHelper::parseBudget)
                .filter(budget -> Objects.nonNull(budget) && budget > 0)
                .distinct()
                .collect();

        // for each budget
        //   total number of times it shows up in a successful movie
        //   calc avg number of times it shows up in a successful movie
        //   calc variance, std dev, confidence interval


    }
}
