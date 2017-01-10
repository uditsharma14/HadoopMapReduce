public class StringUtils {
	public static boolean startsWithDigit(String s) {
		if (s == null || s.length() == 0)
			return false;
		return Character.isDigit(s.charAt(0));
	}

	public static boolean startsWithLetter(String s) {
		if (s == null || s.length() == 0)
			return false;
		return Character.isLetter(s.charAt(0));
	}
}
