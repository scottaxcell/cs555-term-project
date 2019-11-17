package cs555.project.drivers;

import cs555.project.helpers.CreditsHelper;
import cs555.project.helpers.MoviesMetadataHelper;
import cs555.project.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class CastDriver extends Driver {
    public static void main(String[] args) {
        new CastDriver().run();
    }

    private final Map<String, Stats> characterToStats = new HashMap<>();

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
        final String cast;

        public CastMetadata(int id, String cast) {
            this.id = id;
            this.cast = cast;
        }
    }

    private void run() {
        SparkConf conf = new SparkConf().setAppName("Cast Analysis");
//        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("Cast Analysis");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile(HDFS_MOVIES_METADATA);
//        JavaRDD<String> rdd = sc.textFile("/s/chopin/a/grad/sgaxcell/cs555-term-project/data/movies_metadata.csv");

        List<MovieMetadata> allMovies = rdd.map(Utils::splitCommaDelimitedString)
            .filter(split -> MoviesMetadataHelper.isRowValid(split) &&
                MoviesMetadataHelper.parseId(split) != null)
            .map(split -> new MovieMetadata(MoviesMetadataHelper.parseId(split), MoviesMetadataHelper.isMovieSuccessfulByVoteAverage(split)))
            .collect();

        rdd = sc.textFile(HDFS_CREDITS);
//        textFile = sc.textFile("/s/chopin/a/grad/sgaxcell/cs555-term-project/data/credits.csv");
        List<CastMetadata> castMetadatas = rdd.map(Utils::splitCommaDelimitedString)
            .filter(split -> CreditsHelper.isRowValid(split) &&
                CreditsHelper.parseId(split) != null)
            .map(split -> new CastMetadata(CreditsHelper.parseId(split), CreditsHelper.parseCast(split)))
            .collect();

        ObjectMapper objectMapper = new ObjectMapper();

        castMetadatas.stream()
            .filter(castMetadata -> !castMetadata.cast.equals("[]"))
            .forEach(castMetadata -> {
                MovieMetadata movieMetadata = getMovieMetadata(allMovies, castMetadata.id);
                if (movieMetadata != null) {
                    String fixedCast = castMetadata.cast.substring(1, castMetadata.cast.length() - 1)
                        .replace(": None", ": \'\'")
                        .replace("\\", "")
                        .replace("\"\"", "\"");
                    try {
                        JsonNode jsonNode = objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true).readTree(fixedCast);
                        Iterator<JsonNode> iterator = jsonNode.iterator();
                        while (iterator.hasNext()) {
                            JsonNode node = iterator.next();
                            JsonNode characterNode = node.get("character");
                            if (characterNode != null && characterNode.isTextual()) {
                                String character = characterNode.getTextValue();
                                if (Utils.isStringValid(character)) {
                                    Stats stats = characterToStats.computeIfAbsent(character, k -> new Stats());
                                    if (movieMetadata.successful)
                                        stats.numSuccessful++;
                                    stats.numMovies++;
                                }
                            }
                        }
                    }
                    catch (IOException ignored) {
                    }
                }
            });

        calculatePopulationMeanAndStdDev(allMovies);

        characterToStats.entrySet().stream()
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

    private MovieMetadata getMovieMetadata(List<MovieMetadata> movieMetadatas, int id) {
        for (MovieMetadata movieMetadata : movieMetadatas) {
            if (movieMetadata.id == id) {
                return movieMetadata;
            }
        }
        return null;
    }

    private Stats buildOtherStats(String key) {
        Stats stats = new Stats();
        characterToStats.entrySet().stream()
            .filter(entry -> !entry.getKey().equals(key))
            .forEach(entry -> {
                stats.numMovies += entry.getValue().numMovies;
                stats.numSuccessful += entry.getValue().numSuccessful;
            });
        return stats;
    }

    private void writeStatisticsToFile(JavaSparkContext sc) {
        List<String> writeMe = new ArrayList<>();
        writeMe.add("Movie Character Name Analysis");
        writeMe.add("=============================\n");
        writeMe.add("Z values > confidence score of 1.96 are statistically significant");
        writeMe.add("-----------------------------------------------------------------\n");

        characterToStats.entrySet().stream()
            .filter(entry -> entry.getValue().numMovies > 100)
            .sorted((e1, e2) -> e1.getValue().compareTo(e2.getValue()))
            .forEach(e -> {
                Stats stats = e.getValue();
                writeMe.add(String.format("%s: %s", e.getKey(), stats));
            });

        sc.parallelize(writeMe, 1).saveAsTextFile("CastAnalysis");
    }

    /**
     * Estimate population mean using sample size
     *
     * @param movieMetadatas
     */
    private void calculatePopulationMeanAndStdDev(List<MovieMetadata> movieMetadatas) {
        movieMetadatas.stream()
            .forEach(movieMetadata -> {
                if (movieMetadata.successful)
                    numSuccessful++;
                numMovies++;
            });

        populationMean = numSuccessful / (float) numMovies;

        stdDev = (float) Math.sqrt(2 * populationMean * (1 - populationMean) / numMovies);
    }
}
