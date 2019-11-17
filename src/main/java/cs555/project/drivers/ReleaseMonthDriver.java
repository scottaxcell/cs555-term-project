package cs555.project.drivers;

import cs555.project.helpers.MoviesMetadataHelper;
import cs555.project.utils.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReleaseMonthDriver extends Driver {
    public static void main(String[] args) {
        new ReleaseMonthDriver().run();
    }

    private final Map<Integer, Stats> monthToStats = new HashMap<>();

    private static class ReleaseMonthMetadata implements Serializable {
        final int month;
        final boolean successful;

        public ReleaseMonthMetadata(int month, boolean successful) {
            this.month = month;
            this.successful = successful;
        }
    }

    private void run() {
        SparkConf conf = new SparkConf().setAppName("Release Month Analysis");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile(HDFS_MOVIES_METADATA);
//        JavaRDD<String> rdd = sc.textFile("/s/chopin/a/grad/sgaxcell/cs555-term-project/data/movies_metadata.csv");

        List<ReleaseMonthMetadata> allMoviesWithAReleaseMonth = rdd.map(Utils::splitCommaDelimitedString)
            .filter(split -> MoviesMetadataHelper.isRowValid(split) &&
                MoviesMetadataHelper.parseReleaseMonth(split) != -1)
            .map(split -> new ReleaseMonthMetadata(MoviesMetadataHelper.parseReleaseMonth(split), MoviesMetadataHelper.isMovieSuccessfulByVoteAverage(split)))
            .collect();

        allMoviesWithAReleaseMonth.stream()
            .forEach(releaseMonthMetadata -> {
                int releaseMonth = releaseMonthMetadata.month;
                Stats stats = monthToStats.computeIfAbsent(releaseMonth, k -> new Stats());
                if (releaseMonthMetadata.successful)
                    stats.numSuccessful++;
                stats.numMovies++;
            });

        calculatePopulationMeanAndStdDev(allMoviesWithAReleaseMonth);

        monthToStats.entrySet().stream()
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
        monthToStats.entrySet().stream()
            .filter(entry -> !entry.getKey().equals(key))
            .forEach(entry -> {
                stats.numMovies += entry.getValue().numMovies;
                stats.numSuccessful += entry.getValue().numSuccessful;
            });
        return stats;
    }

    private void writeStatisticsToFile(JavaSparkContext sc) {
        List<String> writeMe = new ArrayList<>();
        writeMe.add("Release Month Analysis");
        writeMe.add("=======================\n");
        writeMe.add("Z values > confidence score of 1.96 are statistically significant");
        writeMe.add("-----------------------------------------------------------------\n");

        monthToStats.entrySet().stream()
            .filter(entry -> entry.getValue().numMovies > 100)
            .sorted((e1, e2) -> e1.getValue().compareTo(e2.getValue()))
            .forEach(e -> {
                Stats stats = e.getValue();
                writeMe.add(String.format("%s: %s", e.getKey(), stats));
            });

        sc.parallelize(writeMe, 1).saveAsTextFile("ReleaseMonthAnalysis");
    }

    /**
     * Estimate population mean using sample size
     *
     * @param overviewMetadatas
     */
    private void calculatePopulationMeanAndStdDev(List<ReleaseMonthMetadata> releaseMonthMetadatas) {
    	releaseMonthMetadatas.stream()
            .forEach(releaseMonthMetadata -> {
                if (releaseMonthMetadata.successful)
                    numSuccessful++;
                numMovies++;
            });

        populationMean = numSuccessful / (float) numMovies;

        stdDev = (float) Math.sqrt(2 * populationMean * (1 - populationMean) / numMovies);
    }

}
