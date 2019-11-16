package cs555.project.helpers;

import cs555.project.utils.Utils;

public class CreditsHelper {
    public static final int CAST_INDEX = 0;
    public static final int CREW_INDEX = 1;
    public static final int ID_INDEX = 2;
    public static final int NUM_FIELDS = 3;

    public static boolean isRowValid(String[] split) {
        if (split.length != NUM_FIELDS)
            return false;
        if (split[0].equalsIgnoreCase("cast")) // header
            return false;
        return true;
    }

    public static String parseCast(String[] split) {
        String cast = split[CAST_INDEX];
        return Utils.isStringValid(cast) ? cast : null;
    }

    public static String parseCrew(String[] split) {
        String crew = split[CREW_INDEX];
        return Utils.isStringValid(crew) ? crew : null;
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
}
