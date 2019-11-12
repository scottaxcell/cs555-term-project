package cs555.project;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cs555.project.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.mortbay.util.ajax.JSON;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CastDriver extends Driver {
    public static void main(String[] args) {
        new CastDriver().run();
    }

    private final Map<String, Stats> wordToStats = new HashMap<>();

    private static class MovieMetadata implements Serializable {
        final int id;
        final boolean successful;

        public MovieMetadata(int id, boolean successful) {
            this.id = id;
            this.successful = successful;
        }
    }

    private static class CastMetadata implements Serializable {
        final int id;
        final JsonNode cast;
        final boolean successful;

        public CastMetadata(int id, JsonNode cast, boolean successful) {
            this.id = id;
            this.cast = cast;
            this.successful = successful;
        }
    }

    private void run() {
//        SparkConf conf = new SparkConf().setAppName("Cast Analysis");
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Cast Analysis");

        JavaSparkContext sc = new JavaSparkContext(conf);

//        JavaRDD<String> textFile = sc.textFile(TBD/data/movies_metadata.csv);
        JavaRDD<String> textFile = sc.textFile("/s/chopin/a/grad/sgaxcell/cs555-term-project/data/movies_metadata.csv");

        List<MovieMetadata> allMovies = textFile.map(Utils::splitCommaDelimitedString)
            .filter(split -> MoviesMetadataHelper.isRowValid(split) &&
                MoviesMetadataHelper.parseId(split) != null)
            .map(split -> new MovieMetadata(MoviesMetadataHelper.parseId(split), MoviesMetadataHelper.isMovieSuccessfulByVoteAverage(split)))
            .collect();

        Broadcast<List<MovieMetadata>> allMoviesMetadataBroadcast = sc.broadcast(allMovies);

        ObjectMapper objectMapper = new ObjectMapper();
        Broadcast<ObjectMapper> objectMapperBroadcast = sc.broadcast(objectMapper);

//        textFile = sc.textFile(TBD/data/credist.csv);
        textFile = sc.textFile("/s/chopin/a/grad/sgaxcell/cs555-term-project/data/credits.csv");
        textFile.map(Utils::splitCommaDelimitedString)
            .filter(split -> MoviesMetadataHelper.isRowValid(split) &&
                MoviesMetadataHelper.parseId(split) != null)
            .map(split -> {
                objectMapperBroadcast.try {
                    JsonNode jsonNode = objectMapper.readTree(json);
                    Utils.debug(jsonNode);
                }
                catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            })
            .collect();
//        JsonNode string = objectMapper.readTree("string");
//        allMovies.stream()
//            .forEach(movieMetadata -> {
//                String[] words = movieMetadata.overview.split(" ");
//                for (String word : words) {
//                    Stats stats = wordToStats.computeIfAbsent(word, k -> new Stats());
//                    if (movieMetadata.successful)
//                        stats.numSuccessful++;
//                    stats.numMovies++;
//                }
//            });
//
//        calculatePopulationMeanAndStdDev(allMovies);
//
//        wordToStats.entrySet().stream()
//            .filter(entry -> entry.getValue().numMovies > 100)
//            .forEach(entry -> {
//                Stats otherStats = buildOtherStats(entry.getKey());
//                float p1 = entry.getValue().getSuccessProportion();
//                float p2 = otherStats.getSuccessProportion();
//                float confidenceInterval = p1 - p2;
//                entry.getValue().confidenceInterval = confidenceInterval;
//
//                float z = calculateZ(confidenceInterval);
//                entry.getValue().z = z;
//            });

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
     * @param movieMetadata
     */
    private void calculatePopulationMeanAndStdDev(List<MovieMetadata> movieMetadata) {
//        movieMetadata.stream()
//            .forEach(movieMetadata -> {
//                if (movieMetadata.successful)
//                    numSuccessful++;
//                numMovies++;
//            });

        populationMean = numSuccessful / (float) numMovies;

        stdDev = (float) Math.sqrt(2 * populationMean * (1 - populationMean) / numMovies);
    }
}
