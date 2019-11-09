package cs555.project;

import cs555.project.utils.TotalAndCount;
import cs555.project.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;

public class BudgetDriver {
    public static void main(String[] args) {
        new BudgetDriver().run();
    }

    private final Map<Integer, TotalAndCount> budgetToCount = new HashMap<>();
    private final Map<Integer, BudgetStats> budgetToStats = new HashMap<>();
    private int totalNumSuccessfulMovies = 0;

    private void run() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Budget Analysis");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("/s/chopin/a/grad/sgaxcell/cs555-term-project/data/movies_metadata.csv");

        // find unique budgets; budget is considered valid if it is greater than 0
        textFile.map(Utils::splitCommaDelimitedString)
            .filter(split -> MoviesMetadataHelper.isRowValid(split) &&
                                 MoviesMetadataHelper.isMovieSuccessfulByVoteAverage(split))
            .map(MoviesMetadataHelper::parseBudget)
            .filter(budget -> Objects.nonNull(budget) && budget > 0)
            .foreach(budget -> {
                TotalAndCount tac = budgetToCount.computeIfAbsent(budget, k -> new TotalAndCount());
                tac.count++;
                tac.total += budget;
                totalNumSuccessfulMovies++;
            });

        budgetToCount.entrySet().stream()
            .forEach(e -> {
                BudgetStats budgetStats = new BudgetStats(e.getValue().count, totalNumSuccessfulMovies);
                budgetToStats.put(e.getKey(), budgetStats);
            });

        writeStatisticsToFile(sc);
    }

    private void writeStatisticsToFile(JavaSparkContext sc) {
        List<String> writeMe = new ArrayList<>();
        writeMe.add("Budget 0.95 Confidence Interval Successful Movie");
        writeMe.add("================================================");
        writeMe.add("Budgets outside of the confidence interval are abonormal");
        writeMe.add("--------------------------------------------------------");
        writeMe.add("Budget,Count,Average,Standard Deviation,Confidence Interval");

        // todo(sga) -- write out pretty and csv flavors
        budgetToStats.entrySet().stream()
            .forEach(e -> {
                BudgetStats stats = e.getValue();
                writeMe.add(e.getKey() + "," + stats.count + "," + stats.average + "," + "," + stats.standardDeviation + "," + stats.confidenceInterval);
            });
        sc.parallelize(writeMe, 1).saveAsTextFile("BudgetAnalysis");
    }

    private static class BudgetStats {
        static final float ZEE = 1.960f; // for confidence interval of 0.95
        final int count;
        final int totalNumSuccessfulMovies;
        float average;
        float variance;
        float standardDeviation;
        float confidenceInterval;

        private BudgetStats(int count, int totalNumSuccessfulMovies) {
            this.count = count;
            this.totalNumSuccessfulMovies = totalNumSuccessfulMovies;
            calculateAverage();
            calculateVariance();
            calculateStandardDeviation();
            calculateConfidenceInterval();
        }

        private void calculateConfidenceInterval() {
            confidenceInterval = (float) (ZEE * standardDeviation / Math.sqrt(totalNumSuccessfulMovies));
        }

        private void calculateStandardDeviation() {
            standardDeviation = (float) Math.sqrt(variance);
        }

        private void calculateVariance() {
            variance = (float) (Math.pow((count - average), 2) / totalNumSuccessfulMovies);
        }

        private void calculateAverage() {
            average = count / totalNumSuccessfulMovies;
        }
    }
}
