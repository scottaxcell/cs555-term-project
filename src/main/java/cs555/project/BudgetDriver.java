package cs555.project;

import cs555.project.utils.TotalAndCount;
import cs555.project.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.*;

public class BudgetDriver implements Serializable {
    public static void main(String[] args) {
        new BudgetDriver().run();
    }

    // confidence interval for p1 - p2, where px = # success / # movies
    // std dev. = sqrt(2p (1 - p) / n1 + n2), where p = (s1 + s2 / n1 + n2)
    // z = (conf. int. - 0) / std. dev.
    // if z > Zc then budget is more likely to produce a successful movie, if s1 > s2 that is

    private final Map<Integer, BudgetStats> budgetToStats = new HashMap<>();
    private int totalNumSuccessfulMovies;
    private int totalNumMovies;
    private float populationMean;
    private float stdDev;

    private static class BudgetStats implements Comparable {
        int numMovies;
        int numSuccessful;
        float confidenceInterval;
        float z;

        float getSuccessProportion() {
            return numSuccessful / (float) numMovies;
        }

        @Override
        public String toString() {
            return "BudgetStats{" +
                "numMovies=" + numMovies +
                ", numSuccessful=" + numSuccessful +
                ", confidenceInterval=" + String.format("%.2f", confidenceInterval) +
                ", z=" + String.format("%.2f", z) +
                '}';
        }

        @Override
        public int compareTo(Object o) {
            return Float.compare(z, ((BudgetStats) o).z);
        }
    }

    private static class BudgetMetadata implements Serializable {
        final int budget;
        final boolean successful;

        public BudgetMetadata(int budget, boolean successful) {
            this.budget = budget;
            this.successful = successful;
        }
    }

    private void run() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Budget Analysis");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("/s/chopin/a/grad/sgaxcell/cs555-term-project/data/movies_metadata.csv");

        List<BudgetMetadata> allMoviesWithABudget = textFile.map(Utils::splitCommaDelimitedString)
            .filter(split -> MoviesMetadataHelper.isRowValid(split) &&
                MoviesMetadataHelper.parseBudget(split) != null)
            .map(split -> new BudgetMetadata(MoviesMetadataHelper.parseBudget(split), MoviesMetadataHelper.isMovieSuccessfulByVoteAverage(split)))
            .collect();

        allMoviesWithABudget.stream()
            .forEach(budgetMetadata -> {
                BudgetStats budgetStats = budgetToStats.computeIfAbsent(budgetMetadata.budget, k -> new BudgetStats());
                if (budgetMetadata.successful)
                    budgetStats.numSuccessful++;
                budgetStats.numMovies++;
            });

        calculatePopulationMeanAndStdDev(allMoviesWithABudget);

        budgetToStats.entrySet().stream()
            .filter(entry -> entry.getValue().numMovies > 100)
            .forEach(entry -> {
                BudgetStats otherStats = buildOtherStats(entry.getKey());
                float p1 = entry.getValue().getSuccessProportion();
                float p2 = otherStats.getSuccessProportion();
                float confidenceInterval = p1 - p2;
                entry.getValue().confidenceInterval = confidenceInterval;

                float z = calculateZ(confidenceInterval);
                entry.getValue().z = z;
            });

        writeStatisticsToFile(sc);
    }

    private float calculateZ(float confidenceInterval) {
        return confidenceInterval / stdDev;
    }

    private BudgetStats buildOtherStats(Integer key) {
        BudgetStats budgetStats = new BudgetStats();
        budgetToStats.entrySet().stream()
            .filter(entry -> !entry.getKey().equals(key))
            .forEach(entry -> {
                budgetStats.numMovies += entry.getValue().numMovies;
                budgetStats.numSuccessful += entry.getValue().numSuccessful;
            });
        return budgetStats;
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
                BudgetStats stats = e.getValue();
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
                    totalNumSuccessfulMovies++;
                totalNumMovies++;
            });
        populationMean = totalNumSuccessfulMovies / (float) totalNumMovies;

        stdDev = (float) Math.sqrt(2 * populationMean * (1 - populationMean) / totalNumMovies);
    }
}
