package cs555.project.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {
    private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");

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
}
