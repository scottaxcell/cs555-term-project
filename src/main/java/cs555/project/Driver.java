package cs555.project;

import java.io.Serializable;

/**
 * confidence interval = p1 - p2, where px = # success / # movies
 * standard deviation = sqrt(2p (1 - p) / n1 + n2), where p = (s1 + s2 / n1 + n2)
 * z = (confidence interval - 0) / standard. deviation
 * if z > zc then p1 is more likely to produce a successful movie, if s1 > s2 that is
 * zc = 1.96 for 95% confidence
 */
class Driver implements Serializable {
    int numSuccessful;
    int numMovies;
    float populationMean;
    float stdDev;

    float calculateZ(float confidenceInterval) {
        return confidenceInterval / stdDev;
    }
}
