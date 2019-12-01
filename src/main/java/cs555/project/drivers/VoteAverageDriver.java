package cs555.project.drivers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;

import cs555.project.helpers.MoviesMetadataHelper;
import cs555.project.utils.Utils;

public class VoteAverageDriver extends Driver {
	public static void main(String[] args) {
        new VoteAverageDriver().run();
    }

   // private final Map<String, GrossStats> moviesWithGrossStats = new HashMap<>();
   
   private final Map<Integer, Integer> movieReviewCounts = new HashMap<>();

    private static class GrossMetadata implements Serializable {
        final String title;
        final GrossStats grossStats;
        final double rating;

        public GrossMetadata(String title, GrossStats grossStats, double rating) {
            this.title = title;
            this.grossStats = grossStats;
            this.rating = rating;
        }
    }

    private void run() {
        SparkConf conf = new SparkConf().setMaster("spark://pierre:42600").setAppName("Review Distribution Analysis");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("hdfs://pierre:42500/movies_dataset/movies_metadata.csv");

        JavaRDD<Double> allMoviesWithReview = textFile.map(Utils::splitCommaDelimitedString)
            .filter(split -> MoviesMetadataHelper.isRowValid(split) && !(MoviesMetadataHelper.getVoteAverage(split) < 0))
            .map(split ->  ((int)(MoviesMetadataHelper.getVoteAverage(split)*10))/10d); // rounded to 2 decimal places

         JavaPairRDD<Double, Integer> frequencies = allMoviesWithReview.mapToPair(entry -> new Tuple2(entry,1));

         Map<Double, Long> test = frequencies.countByKey();

         List<String> writeMe = new ArrayList<>();

         double sum = 0;
         int count = 0;

         for (Map.Entry<Double, Long> entry : test.entrySet()) {
            if (entry.getKey() < .1) continue;
            sum += entry.getKey()*entry.getValue();
            count += entry.getValue();
            writeMe.add(entry.getKey() + " " + entry.getValue());
         }

         double mean = sum/count;

         double sumDiff = 0;

         for (Map.Entry<Double, Long> entry : test.entrySet()) {
            if (entry.getKey() < .1) continue;
            sumDiff += Math.pow(entry.getKey() - mean,2)*entry.getValue();
         }

         double meanDiff = sumDiff/count;

         double stdDev = Math.pow(meanDiff, .5);

         writeMe.add("average review: " + sum/count);
         writeMe.add("standard deviation: "  + stdDev);

         // 1. Work out the Mean (the simple average of the numbers)
         // 2. Then for each number: subtract the Mean and square the result
         // 3. Then work out the mean of those squared differences.
         // 4. Take the square root of that and we are done!

         sc.parallelize(writeMe, 1).saveAsTextFile("hdfs://pierre:42500/freqAndAverage");
        
    }

}
