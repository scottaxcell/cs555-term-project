package cs555.project.drivers;

import cs555.project.helpers.MoviesMetadataHelper;
import cs555.project.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BudgetDriver extends Driver {
    public static void main(String[] args) {
        new BudgetDriver().run();
    }

    private final Map<Integer, Stats> budgetToStats = new HashMap<>();

    private static class BudgetMetadata implements Serializable {
        final int budget;
        final boolean successful;

        BudgetMetadata(int budget, boolean successful) {
            this.budget = budget;
            this.successful = successful;
        }
    }

    private void run() {
//        SparkConf conf = new SparkConf().setAppName("Budget Analysis");
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Budget Analysis");

        JavaSparkContext sc = new JavaSparkContext(conf);

//        JavaRDD<String> textFile = sc.textFile(TBD/data/movies_metadata.csv);
        JavaRDD<String> textFile = sc.textFile("/s/chopin/a/grad/sgaxcell/cs555-term-project/data/movies_metadata.csv");

        List<BudgetMetadata> allMoviesWithABudget = textFile.map(Utils::splitCommaDelimitedString)
            .filter(split -> MoviesMetadataHelper.isRowValid(split) &&
                MoviesMetadataHelper.parseBudget(split) != null)
            .map(split -> new BudgetMetadata(MoviesMetadataHelper.parseBudget(split), MoviesMetadataHelper.isMovieSuccessfulByVoteAverage(split)))
            .collect();

        allMoviesWithABudget.stream()
            .forEach(budgetMetadata -> {
                Stats stats = budgetToStats.computeIfAbsent(budgetMetadata.budget, k -> new Stats());
                if (budgetMetadata.successful)
                    stats.numSuccessful++;
                stats.numMovies++;
            });

        calculatePopulationMeanAndStdDev(allMoviesWithABudget);

        budgetToStats.entrySet().stream()
            .filter(entry -> entry.getValue().numMovies > 100)
            .forEach(entry -> {
                Stats otherStats = buildOtherStats(entry.getKey());
                float p1 = entry.getValue().getSuccessProportion();
                float p2 = otherStats.getSuccessProportion();
                float confidenceInterval = p1 - p2;
                entry.getValue().confidenceInterval = confidenceInterval;

                float z = calculateZ(confidenceInterval);
                entry.getValue().z = z;
            });

        writeStatisticsToFile(sc);
    }

    private Stats buildOtherStats(Integer key) {
        Stats stats = new Stats();
        budgetToStats.entrySet().stream()
            .filter(entry -> !entry.getKey().equals(key))
            .forEach(entry -> {
                stats.numMovies += entry.getValue().numMovies;
                stats.numSuccessful += entry.getValue().numSuccessful;
            });
        return stats;
    }

    private void writeStatisticsToFile(JavaSparkContext sc) {
        List<String> writeMe = new ArrayList<>();
        writeMe.add("Movie Budget Analysis");
        writeMe.add("=====================\n");
        writeMe.add("Z values > confidence score of 1.96 are statistically significant");
        writeMe.add("-----------------------------------------------------------------\n");

        budgetToStats.entrySet().stream()
            .filter(entry -> entry.getValue().numMovies > 100)
            .sorted((e1, e2) -> e1.getValue().compareTo(e2.getValue()))
            .forEach(e -> {
                Stats stats = e.getValue();
                writeMe.add(String.format("%d: %s", e.getKey(), stats));
            });

        sc.parallelize(writeMe, 1).saveAsTextFile("BudgetAnalysis");
    }

    /**
     * Estimate population mean using sample size
     *
     * @param budgetMetadatas
     */
    private void calculatePopulationMeanAndStdDev(List<BudgetMetadata> budgetMetadatas) {
        budgetMetadatas.stream()
            .forEach(budgetMetadata -> {
                if (budgetMetadata.successful)
                    numSuccessful++;
                numMovies++;
            });

        populationMean = numSuccessful / (float) numMovies;

        stdDev = (float) Math.sqrt(2 * populationMean * (1 - populationMean) / numMovies);
    }
}
