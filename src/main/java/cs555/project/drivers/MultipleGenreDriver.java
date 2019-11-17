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
import java.util.Set;
import java.util.HashSet;

public class MultipleGenreDriver extends Driver {
    public static void main(String[] args) {
        new MultipleGenreDriver().run();
    }

    private final Map<Set<String>, Stats> genreToStats = new HashMap<Set<String>, Stats>();

    private static class GenreMetadata implements Serializable {
        final HashSet<String> genres;
        final boolean successful;

        GenreMetadata(HashSet<String> genres, boolean successful) {
            this.genres = genres;
            this.successful = successful;
        }
    }

    private void run() {
        SparkConf conf = new SparkConf().setMaster("spark://pierre:42600").setAppName("Single Genre Analysis");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("hdfs://pierre:42500/movies_dataset/movies_metadata.csv");

        List<GenreMetadata> allMoviesWithAGenre = textFile.map(Utils::splitCommaDelimitedString)
            .filter(split -> MoviesMetadataHelper.isRowValid(split) &&
                MoviesMetadataHelper.parseGenres(split) != null)
            .map(split -> new GenreMetadata(MoviesMetadataHelper.parseGenres(split), MoviesMetadataHelper.isMovieSuccessfulByVoteAverage(split)))
            .collect();

         allMoviesWithAGenre.stream()
         .forEach(genreReleaseMetadata -> {
               Stats stats = genreToStats.computeIfAbsent(genreReleaseMetadata.genres, k -> new Stats());
               if (genreReleaseMetadata.successful)
                  stats.numSuccessful++;
               stats.numMovies++;    
         });     

        calculatePopulationMeanAndStdDev(allMoviesWithAGenre);

        genreToStats.entrySet().stream()
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

    private Stats buildOtherStats(Set<String> key) {
        Stats stats = new Stats();
        genreToStats.entrySet().stream()
            .filter(entry -> !entry.getKey().equals(key))
            .forEach(entry -> {
                stats.numMovies += entry.getValue().numMovies;
                stats.numSuccessful += entry.getValue().numSuccessful;
            });
        return stats;
    }

    private void writeStatisticsToFile(JavaSparkContext sc) {
        List<String> writeMe = new ArrayList<>();
        writeMe.add("Movie Multiple Genre Analysis");
        writeMe.add("=====================\n");
        writeMe.add("Z values > confidence score of 1.96 are statistically significant");
        writeMe.add("-----------------------------------------------------------------\n");

        genreToStats.entrySet().stream()
            .filter(entry -> entry.getValue().numMovies > 100)
            .sorted((e1, e2) -> e1.getValue().compareTo(e2.getValue()))
            .forEach(e -> {
                Stats stats = e.getValue();
                writeMe.add(String.format("%s: %s", e.getKey(), stats));
            });

            sc.parallelize(writeMe, 1).saveAsTextFile("hdfs://pierre:42500/GenreAnalysis");
    }

    /**
     * Estimate population mean using sample size
     *
     * @param genreMetadatas
     */
    private void calculatePopulationMeanAndStdDev(List<GenreMetadata> genreMetadatas) {
        genreMetadatas.stream()
            .forEach(genreMetadata -> {
                if (genreMetadata.successful)
                    numSuccessful++;
                numMovies++;
            });

        populationMean = numSuccessful / (float) numMovies;

        stdDev = (float) Math.sqrt(2 * populationMean * (1 - populationMean) / numMovies);
    }
}
