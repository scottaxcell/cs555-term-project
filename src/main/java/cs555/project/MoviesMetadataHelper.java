package cs555.project;

public class MoviesMetadataHelper {
    public static final int ID_INDEX = 5;
    public static final int ORIGINAL_TITLE_INDEX = 8;
    public static final int OVERVIEW_INDEX = 9;
    public static final int REVENUE_INDEX = 15;

    public static final int NUM_FIELDS = 24;

    public static boolean isRowValid(String[] split) {
        if (split.length != NUM_FIELDS)
            return false;
        if (split[0].equalsIgnoreCase("adult")) // header
            return false;
        return true;
    }
}
