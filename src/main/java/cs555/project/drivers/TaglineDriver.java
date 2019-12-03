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

public class TaglineDriver extends Driver {
    public static void main(String[] args) {
        new TaglineDriver().run();
    }

    private final Map<String, Stats> wordToStats = new HashMap<>();

    private static class TaglineMetadata implements Serializable {
        final String tagline;
        final boolean successful;

        public TaglineMetadata(String tagline, boolean successful) {
            this.tagline = tagline;
            this.successful = successful;
        }
    }

    private void run() {
        SparkConf conf = new SparkConf().setAppName("Tagline Analysis");
//        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("Tagline Analysis");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile(HDFS_MOVIES_METADATA);
//        JavaRDD<String> rdd = sc.textFile("/s/chopin/a/grad/sgaxcell/cs555-term-project/data/movies_metadata.csv");

        List<TaglineMetadata> allMoviesWithATagline = rdd.map(Utils::splitCommaDelimitedString)
            .filter(split -> MoviesMetadataHelper.isRowValid(split) &&
                MoviesMetadataHelper.parseTagline(split) != null)
            .map(split -> new TaglineMetadata(MoviesMetadataHelper.parseTagline(split), MoviesMetadataHelper.isMovieSuccessfulByVoteAverage(split)))
            .collect();

        allMoviesWithATagline.stream()
            .forEach(taglineMetadata -> {
                String[] words = taglineMetadata.tagline.split(" ");
                for (String word : words) {
                    Stats stats = wordToStats.computeIfAbsent(MoviesMetadataHelper.parseWord(word), k -> new Stats());
                    if (taglineMetadata.successful)
                        stats.numSuccessful++;
                    stats.numMovies++;
                }
            });

        calculatePopulationMeanAndStdDev(allMoviesWithATagline);

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
        writeMe.add("Movie Tagline Analysis");
        writeMe.add("======================\n");
        writeMe.add("Z values > confidence score of 1.96 are statistically significant");
        writeMe.add("-----------------------------------------------------------------\n");

        wordToStats.entrySet().stream()
            .filter(entry -> entry.getValue().numMovies > 100)
            .sorted((e1, e2) -> e1.getValue().compareTo(e2.getValue()))
            .forEach(e -> {
                Stats stats = e.getValue();
                writeMe.add(String.format("%s: %s", e.getKey(), stats));
            });

        sc.parallelize(writeMe, 1).saveAsTextFile("TaglineAnalysis");
    }

    /**
     * Estimate population mean using sample size
     *
     * @param taglineMetadatas
     */
    private void calculatePopulationMeanAndStdDev(List<TaglineMetadata> taglineMetadatas) {
        taglineMetadatas.stream()
            .forEach(taglineMetadata -> {
                if (taglineMetadata.successful)
                    numSuccessful++;
                numMovies++;
            });

        populationMean = numSuccessful / (float) numMovies;

        stdDev = (float) Math.sqrt(2 * populationMean * (1 - populationMean) / numMovies);
    }
}
