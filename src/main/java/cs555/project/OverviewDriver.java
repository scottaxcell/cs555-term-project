package cs555.project;

import cs555.project.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OverviewDriver extends Driver {
    public static void main(String[] args) {
        new OverviewDriver().run();
    }

    private final Map<String, Stats> wordToStats = new HashMap<>();

    private static class OverviewMetadata implements Serializable {
        final String overview;
        final boolean successful;

        public OverviewMetadata(String overview, boolean successful) {
            this.overview = overview;
            this.successful = successful;
        }
    }

    private void run() {
//        SparkConf conf = new SparkConf().setAppName("Overview Analysis");
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Overview Analysis");

        JavaSparkContext sc = new JavaSparkContext(conf);

//        JavaRDD<String> textFile = sc.textFile(TBD/data/movies_metadata.csv);
        JavaRDD<String> textFile = sc.textFile("/s/chopin/a/grad/sgaxcell/cs555-term-project/data/movies_metadata.csv");

        List<OverviewMetadata> allMoviesWithAnOverview = textFile.map(Utils::splitCommaDelimitedString)
            .filter(split -> MoviesMetadataHelper.isRowValid(split) &&
                MoviesMetadataHelper.parseOverview(split) != null)
            .map(split -> new OverviewMetadata(MoviesMetadataHelper.parseOverview(split), MoviesMetadataHelper.isMovieSuccessfulByVoteAverage(split)))
            .collect();

        allMoviesWithAnOverview.stream()
            .forEach(overviewMetadata -> {
                String[] words = overviewMetadata.overview.split(" ");
                for (String word : words) {
                    Stats stats = wordToStats.computeIfAbsent(word, k -> new Stats());
                    if (overviewMetadata.successful)
                        stats.numSuccessful++;
                    stats.numMovies++;
                }
            });

        calculatePopulationMeanAndStdDev(allMoviesWithAnOverview);

        wordToStats.entrySet().stream()
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

    private Stats buildOtherStats(String key) {
        Stats stats = new Stats();
        wordToStats.entrySet().stream()
            .filter(entry -> !entry.getKey().equals(key))
            .forEach(entry -> {
                stats.numMovies += entry.getValue().numMovies;
                stats.numSuccessful += entry.getValue().numSuccessful;
            });
        return stats;
    }

    private void writeStatisticsToFile(JavaSparkContext sc) {
        List<String> writeMe = new ArrayList<>();
        writeMe.add("Movie Overview Analysis");
        writeMe.add("=======================\n");
        writeMe.add("Z values > confidence score of 1.96 are statistically significant");
        writeMe.add("-----------------------------------------------------------------\n");

        wordToStats.entrySet().stream()
            .filter(entry -> entry.getValue().numMovies > 100)
            .sorted((e1, e2) -> e1.getValue().compareTo(e2.getValue()))
            .forEach(e -> {
                Stats stats = e.getValue();
                writeMe.add(String.format("%s: %s", e.getKey(), stats));
            });

        sc.parallelize(writeMe, 1).saveAsTextFile("OverviewAnalysis");
    }

    /**
     * Estimate population mean using sample size
     *
     * @param overviewMetadatas
     */
    private void calculatePopulationMeanAndStdDev(List<OverviewMetadata> overviewMetadatas) {
        overviewMetadatas.stream()
            .forEach(overviewMetadata -> {
                if (overviewMetadata.successful)
                    numSuccessful++;
                numMovies++;
            });

        populationMean = numSuccessful / (float) numMovies;

        stdDev = (float) Math.sqrt(2 * populationMean * (1 - populationMean) / numMovies);
    }
}
