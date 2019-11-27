package cs555.project.drivers;

import cs555.project.helpers.MoviesMetadataHelper;
import cs555.project.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class GenreReleaseDateDriver extends Driver {
    public static void main(String[] args) {
        new GenreReleaseDateDriver().run(args[0]);
    }

    // each genre has a hashmap of its release month and stats about that release month
    private final Map<String, HashMap<Integer,Stats>> genreReleaseToStats = new HashMap<>();


    private static class GenreMetadata implements Serializable {
      final String genre;
      final int monthReleased;
      final boolean successful;

      GenreMetadata(String genre, int month, boolean successful) {
          this.genre = genre;
          this.monthReleased = month;
          this.successful = successful;
      }
  }

    private void run(String outputFile) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Genre release date analysis");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("hdfs://pierre:42500/movies_dataset/movies_metadata.csv");

         List<GenreMetadata> allMoviesWithAReleaseDate = textFile.map(Utils::splitCommaDelimitedString)
         .filter(split -> MoviesMetadataHelper.isRowValid(split) &&
               MoviesMetadataHelper.parseGenreString(split) != null && MoviesMetadataHelper.parseReleaseMonth(split) != -1)
         .map(split -> new GenreMetadata(MoviesMetadataHelper.parseGenreString(split), MoviesMetadataHelper.parseReleaseMonth(split), MoviesMetadataHelper.isMovieSuccessfulByVoteAverage(split)))
         .collect();

         String result = "";

         allMoviesWithAReleaseDate.stream()
        .filter(movieGenres -> !movieGenres.genre.equals("[]"))
            .forEach(genreMetadata -> {

               String genresString = genreMetadata.genre;
               int monthReleased = genreMetadata.monthReleased;

               String[] genres = genresString.split("},");
               for (String genre : genres) {
                  String[] tokens = genre.split("\'");
                  if (tokens.length >= 5) {

                     HashMap<Integer,Stats> genreMap = genreReleaseToStats.computeIfAbsent(tokens[5], k -> new HashMap<Integer,Stats>());
                     Stats stats = genreMap.computeIfAbsent(monthReleased, k -> new Stats());

                     if (genreMetadata.successful)
                        stats.numSuccessful++;
                     stats.numMovies++;
                  }
               }
            });


        for (Map.Entry<String, HashMap<Integer,Stats>> genreMapEntry : genreReleaseToStats.entrySet()) {
           HashMap<Integer, Stats> monthAndStats = genreMapEntry.getValue();
           calculatePopulationMeanAndStdDevForGenreAllMonths(monthAndStats);
           monthAndStats.entrySet().stream()
            .filter(entry -> entry.getValue().numMovies > 100)
            .forEach(entry -> {
               Stats otherStats = buildOtherStats(genreMapEntry.getKey(), entry.getKey());
               float p1 = entry.getValue().getSuccessProportion();
               float p2 = otherStats.getSuccessProportion();
               float confidenceInterval = p1 - p2;
               entry.getValue().confidenceInterval = confidenceInterval;

               float z = calculateZ(confidenceInterval);
               entry.getValue().z = z;
            });
        }
        writeStatisticsToFile(sc,outputFile);
    }

    // return the number of movies and successful movie stats for all other months for this particular genre
    private Stats buildOtherStats(String genre, Integer key) {
        Stats stats = new Stats();
        HashMap<Integer, Stats> monthReleasedForGenre = genreReleaseToStats.get(genre);
        monthReleasedForGenre.entrySet().stream()
            .filter(entry -> !entry.getKey().equals(key))
            .forEach(entry -> {
                stats.numMovies += entry.getValue().numMovies;
                stats.numSuccessful += entry.getValue().numSuccessful;
            });
        return stats;
    }

    private void writeStatisticsToFile(JavaSparkContext sc, String outputFile) {
        List<String> writeMe = new ArrayList<>();
        writeMe.add("Movie Genre And Release Date Analysis");
        writeMe.add("=====================\n");
        writeMe.add("Z values > confidence score of 1.96 are statistically significant");
        writeMe.add("-----------------------------------------------------------------\n");

        // for each genre, perform a statistical test to see what the best month to release that genre is
        for (Map.Entry<String, HashMap<Integer,Stats>> genreMapEntry : genreReleaseToStats.entrySet()) {
         String genre = genreMapEntry.getKey();
         HashMap<Integer, Stats> monthAndStats = genreMapEntry.getValue();
         monthAndStats.entrySet().stream()
          .filter(entry -> entry.getValue().numMovies > 100)
          .sorted((e1, e2) -> e1.getValue().compareTo(e2.getValue()))
            .forEach(e -> {
                Stats stats = e.getValue();
                writeMe.add(String.format("%s: %s", genre + "\t" + getMonth(e.getKey()), stats));
            });
      }

      sc.parallelize(writeMe, 1).saveAsTextFile("hdfs://pierre:42500/"+outputFile);
    }

    /**
     * Estimate population mean using sample size
     *
     * @param genreReleaseMetadatas
     */
    private void calculatePopulationMeanAndStdDev(List<GenreMetadata> genreReleaseMetadatas) {
      genreReleaseMetadatas.stream()
            .forEach(genreReleaseMetadata -> {
                if (genreReleaseMetadata.successful)
                    numSuccessful++;
                numMovies++;
            });

        populationMean = numSuccessful / (float) numMovies;

        stdDev = (float) Math.sqrt(2 * populationMean * (1 - populationMean) / numMovies);
    }

    private void calculatePopulationMeanAndStdDevForGenreAllMonths(HashMap<Integer, Stats> genreMonthsAndStats) {

      numSuccessful = 0;
      numMovies = 0;
      for (Map.Entry<Integer, Stats> entry : genreMonthsAndStats.entrySet()) {
         Stats stats = entry.getValue();
         numSuccessful += stats.numSuccessful;
         numMovies += stats.numMovies;
      }
      populationMean = numSuccessful / (float) numMovies;
      stdDev = (float) Math.sqrt(2 * populationMean * (1 - populationMean) / numMovies);
    }

    private String getMonth(int month) {
       switch (month) {
         case 1: return "January";
         case 2: return "February";
         case 3: return "March";
         case 4: return "April";
         case 5: return "May";
         case 6: return "June";
         case 7: return "July";
         case 8: return "August";
         case 9: return "September";
         case 10: return "October";
         case 11: return "November";
         default: return "December";
       }
    }
}
