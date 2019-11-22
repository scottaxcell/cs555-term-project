package cs555.project.drivers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import cs555.project.helpers.MoviesMetadataHelper;
import cs555.project.utils.Utils;

public class GrossDriver extends Driver {
	public static void main(String[] args) {
        new GrossDriver().run();
    }

	private final Map<String, GrossStats> moviesWithGrossStats = new HashMap<>();

    private static class GrossMetadata implements Serializable {
        final String title;
        final GrossStats grossStats;

        public GrossMetadata(String title, GrossStats grossStats) {
            this.title = title;
            this.grossStats = grossStats;
        }
    }

    private void run() {
        SparkConf conf = new SparkConf().setAppName("Gross Analysis");
//        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("Title Analysis");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile(HDFS_MOVIES_METADATA);
//        JavaRDD<String> rdd = sc.textFile("/s/chopin/a/grad/sgaxcell/cs555-term-project/data/movies_metadata.csv");

        List<GrossMetadata> allMoviesWithAGross = rdd.map(Utils::splitCommaDelimitedString)
            .filter(split -> MoviesMetadataHelper.isRowValid(split) &&
                MoviesMetadataHelper.parseGrossStatsData(split) != null)
            .map(split -> new GrossMetadata(MoviesMetadataHelper.parseTitle(split), MoviesMetadataHelper.parseGrossStatsData(split)))
            .collect();

        allMoviesWithAGross.stream()
        .forEach(grossMetadata -> {
        	String title = grossMetadata.title;
        	GrossStats grossStats = grossMetadata.grossStats;
        	moviesWithGrossStats.put(title, grossStats);
        });
        
        writeStatisticsToFile(sc);
    }

    private void writeStatisticsToFile(JavaSparkContext sc) {
        List<String> writeMe = new ArrayList<>();
        writeMe.add("Movie Gross Analysis");
        writeMe.add("====================\n");
        writeMe.add("Simple info for each movie's profits (revenue is represented as a ratio of gross to budget)");
        writeMe.add("-----------------------------------------------------------------\n");

        moviesWithGrossStats.entrySet().stream()
        .sorted((e1, e2) -> e1.getValue().compareTo(e2.getValue()))
        .forEach(e -> {
        	GrossStats grossStats = e.getValue();
            writeMe.add(String.format("%s: %s", e.getKey(), grossStats));
        });

        sc.parallelize(writeMe, 1).saveAsTextFile("MovieGrossAnalysis");
    }
}
