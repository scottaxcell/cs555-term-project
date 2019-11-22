package cs555.project.helpers;

import cs555.project.drivers.GrossStats;
import cs555.project.utils.Utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;

public class MoviesMetadataHelper {
    public static final int BUDGET_INDEX = 2;
    public static final int ID_INDEX = 5;
    public static final int ORIGINAL_TITLE_INDEX = 8;
    public static final int OVERVIEW_INDEX = 9;
    public static final int REVENUE_INDEX = 15;
    public static final int RELEASE_DATE_INDEX = 14;
    public static final int GENRE_INDEX = 3;
    public static final int TAGLINE_INDEX = 19;
    public static final int VOTE_AVERAGE_INDEX = 22;
    public static final int NUM_FIELDS = 24;
    public static final String DATE_PATTERN = "yyyy-MM-dd";

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

    public static Integer parseRevenue(String[] split) {
        String revenueStr = split[REVENUE_INDEX];
        try {
            int revenue = Integer.parseInt(revenueStr);
            return revenue > 0 ? revenue : null;
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

    public static String parseTitle(String[] split) {
        String title = split[ORIGINAL_TITLE_INDEX];
        return Utils.isStringValid(title) ? title : null;
    }

    public static int parseReleaseMonth(String[] split) {
      String releaseDateString = split[RELEASE_DATE_INDEX];
      String[] releaseDateTokens = releaseDateString.split("-");
      return releaseDateTokens.length == 3 ? Integer.parseInt(releaseDateTokens[1]) : -1;
   }
    
   public static int parseReleaseWeek(String[] split) {
	  String releaseDateString = split[RELEASE_DATE_INDEX];
      String[] releaseDateTokens = releaseDateString.split("-");
      if (releaseDateTokens.length == 3) {
    	  SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_PATTERN);
    	  try {
			Date date = simpleDateFormat.parse(releaseDateString);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			int weekNumber = calendar.get(Calendar.WEEK_OF_YEAR);
			return weekNumber;
		} catch (ParseException e) {
			e.printStackTrace();
		}
      }
      return -1;
      //return releaseDateTokens.length == 3 ? Integer.parseInt(releaseDateTokens[1]) : -1;
   }

   public static String parseGenreString(String[] split) {
      String genresString = split[GENRE_INDEX];
      return genresString.length() > 0 ? genresString : null;
   }

   public static HashSet<String> parseGenres(String[] split) {
      HashSet<String> movieGenres = new HashSet<String>();
      String genresString = split[GENRE_INDEX];
      String[] genres = genresString.split("},");
      for (String genre : genres) {
         String[] tokens = genre.split("\'");
         if (tokens.length >= 5) movieGenres.add(tokens[5]);
      }
      return movieGenres.size() > 0 ? movieGenres : null;
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
    
    public static GrossStats parseGrossStatsData(String[] split) {
    	GrossStats grossStats = null;
    	if (split.length > 15) {
    		if (!split[BUDGET_INDEX].isEmpty() && !split[REVENUE_INDEX].isEmpty()) {
    			Integer budget = parseBudget(split);
            	Integer revenue = parseRevenue(split);
            	
            	if (budget != null && revenue != null) {
            		
            		grossStats = new GrossStats((int) budget, (int) revenue);
            	}
    		}
    	}
    	return grossStats;
    }
}
