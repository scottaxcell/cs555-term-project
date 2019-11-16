package cs555.project;

import cs555.project.utils.Utils;

public class MoviesMetadataHelper {
    public static final int BUDGET_INDEX = 2;
    public static final int ID_INDEX = 5;
    public static final int ORIGINAL_TITLE_INDEX = 8;
    public static final int OVERVIEW_INDEX = 9;
    public static final int REVENUE_INDEX = 15;
    public static final int TAGLINE_INDEX = 19;
    public static final int VOTE_AVERAGE_INDEX = 22;
    public static final int NUM_FIELDS = 24;

    public static final float VOTE_AVERAGE_SUCCESS_BASELINE = 7.0f;

    public static boolean isRowValid(String[] split) {
        if (split.length != NUM_FIELDS)
            return false;
        if (split[0].equalsIgnoreCase("adult")) // header
            return false;
        return true;
    }

    public static Integer parseBudget(String[] split) {
        String budgetStr = split[BUDGET_INDEX];
        try {
            int budget = Integer.parseInt(budgetStr);
            return budget > 0 ? budget : null;
        }
        catch (NumberFormatException ignored) {
        }
        return null;
    }

    public static String parseOverview(String[] split) {
        String overview = split[OVERVIEW_INDEX];
        return Utils.isStringValid(overview) ? overview : null;
    }

    public static String parseTagline(String[] split) {
        String tagline = split[TAGLINE_INDEX];
        return Utils.isStringValid(tagline) ? tagline : null;
    }

    public static Integer parseId(String[] split) {
        String idStr = split[ID_INDEX];
        try {
            int id = Integer.parseInt(idStr);
            return id > 0 ? id : null;
        }
        catch (NumberFormatException ignored) {
        }
        return null;
    }

    public static boolean isMovieSuccessfulByVoteAverage(String[] split) {
        String voteAverageStr = split[VOTE_AVERAGE_INDEX];
        try {
            float voteAverage = Float.parseFloat(voteAverageStr);
            return voteAverage > VOTE_AVERAGE_SUCCESS_BASELINE;
        }
        catch (NumberFormatException ignored) {
        }
        return false;
    }
}
