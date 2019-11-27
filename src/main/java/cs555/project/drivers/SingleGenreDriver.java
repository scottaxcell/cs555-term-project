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

public class SingleGenreDriver extends Driver {
    public static void main(String[] args) {
        new SingleGenreDriver().run();
    }

    private final Map<String, Stats> genreToStats = new HashMap<>();

    private static class GenreMetadata implements Serializable {
        final String genre;
        final boolean successful;

        GenreMetadata(String genre, boolean successful) {
            this.genre = genre;
            this.successful = successful;
        }
    }

    private void run() {
        SparkConf conf = new SparkConf().setMaster("spark://pierre:42600").setAppName("Single Genre Analysis");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("hdfs://pierre:42500/movies_dataset/movies_metadata.csv");

        List<GenreMetadata> allMoviesWithAGenre = textFile.map(Utils::splitCommaDelimitedString)
            .filter(split -> MoviesMetadataHelper.isRowValid(split) &&
                MoviesMetadataHelper.parseGenreString(split) != null)
            .map(split -> new GenreMetadata(MoviesMetadataHelper.parseGenreString(split), MoviesMetadataHelper.isMovieSuccessfulByVoteAverage(split)))
            .collect();

        allMoviesWithAGenre.stream()
        .filter(movieGenres -> !movieGenres.genre.equals("[]"))
            .forEach(genreMetadata -> {

               String genresString = genreMetadata.genre;
               String[] genres = genresString.split("},");
               for (String genre : genres) {
                  String[] tokens = genre.split("\'");
                  if (tokens.length >= 5) {
                     Stats stats = genreToStats.computeIfAbsent(tokens[5], k -> new Stats());
                     if (genreMetadata.successful)
                        stats.numSuccessful++;
                     stats.numMovies++;
                  }
               }
            });

        calculatePopulationMeanAndStdDev(allMoviesWithAGenre);

        genreToStats.entrySet().stream()
            .filter(entry -> entry.getValue().numMovies > 5)
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
        writeMe.add("Movie Single Genre Analysis");
        writeMe.add("=====================\n");
        writeMe.add("Z values > confidence score of 1.96 are statistically significant");
        writeMe.add("-----------------------------------------------------------------\n");

        genreToStats.entrySet().stream()
            .filter(entry -> entry.getValue().numMovies > 5)
            .sorted((e1, e2) -> e1.getValue().compareTo(e2.getValue()))
            .forEach(e -> {
                Stats stats = e.getValue();
                writeMe.add(String.format("%s: %s", e.getKey(), stats));
            });

            sc.parallelize(writeMe, 1).saveAsTextFile("hdfs://pierre:42500/SingleGenreAnalysis");
    }

    /**
     * Estimate population mean using sample size
     *
     * @param genreMetadatas
     */
    private void calculatePopulationMeanAndStdDev(List<GenreMetadata> genreMetadatas) {
        genreMetadatas.stream()
            .forEach(budgetMetadata -> {
                if (budgetMetadata.successful)
                    numSuccessful++;
                numMovies++;
            });

        populationMean = numSuccessful / (float) numMovies;

        stdDev = (float) Math.sqrt(2 * populationMean * (1 - populationMean) / numMovies);
    }
}
