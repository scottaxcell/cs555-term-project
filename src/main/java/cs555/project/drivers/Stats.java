package cs555.project.drivers;

class Stats implements Comparable {
    int numMovies;
    int numSuccessful;
    float confidenceInterval;
    float z;

    @Override
    public String toString() {
        return "Stats{" +
            "numMovies=" + numMovies +
            ", numSuccessful=" + numSuccessful +
            ", confidenceInterval=" + String.format("%.2f", confidenceInterval) +
            ", z=" + String.format("%.2f", z) +
            '}';
    }

    @Override
    public int compareTo(Object o) {
        return Float.compare(z, ((Stats) o).z);
    }

    float getSuccessProportion() {
        return numSuccessful / (float) numMovies;
    }
}
