package cs555.project.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class Utils {
    private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("MM-dd");

    public static void debug(Object o) {
        System.out.println(String.format("DEBUG: [%s] %s", TIME_FORMAT.format(new Date()), o));
    }

    public static boolean isStringValid(String str) {
        return str != null && !str.isEmpty() && !str.trim().isEmpty();
    }

    /**
     * Splits comma delimited string, ignoring commas inside quotes
     */
    public static String[] splitCommaDelimitedString(String string) {
        StringBuilder sb = new StringBuilder(string);
        boolean inQuotes = false;
        for (int i = 0; i < sb.length(); i++) {
            char c = sb.charAt(i);
            if (c == '\"')
                inQuotes = !inQuotes;
            if (c == ',' && inQuotes)
                sb.setCharAt(i, ';');
        }
        String[] split = sb.toString().split(",", -1);
        for (int i = 0; i < split.length; i++)
            split[i] = split[i].replaceAll(";", ",");
        return split;
    }
    
    public static String getDateFromWeekNumber(int weekNumber, String day) {
		Calendar cal = Calendar.getInstance(Locale.US);
		cal.set(Calendar.WEEK_OF_YEAR, weekNumber);
		
		if (day.equalsIgnoreCase("start")) {
			cal.set(Calendar.DAY_OF_WEEK, Calendar.TUESDAY);
		} else if (day.equalsIgnoreCase("end")) {
			cal.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY);
			cal.add(Calendar.DAY_OF_MONTH, 2);
		}
		
		return DATE_FORMAT.format(cal.getTime());
    }
}
