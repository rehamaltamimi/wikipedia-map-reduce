package net.minidev.json.parser;

/**
 * ParseException explains why and where the error occurs in source JSON text.
 * 
 * @author Uriel Chemouni <uchemouni@gmail.com>
 */
public class ParseException extends Exception {
	private static final long serialVersionUID = 8879024178584091857L;

	public static final int ERROR_UNEXPECTED_CHAR = 0;
	public static final int ERROR_UNEXPECTED_TOKEN = 1;
	// public static final int ERROR_UNEXPECTED_EXCEPTION = 2;
	public static final int ERROR_UNEXPECTED_EOF = 3;
	public static final int ERROR_UNEXPECTED_UNICODE = 4;

	private int errorType;
	private Object unexpectedObject;
	private int position;

	public ParseException(int position, int errorType, Object unexpectedObject) {
		super(toMessage(position, errorType, unexpectedObject));
		this.position = position;
		this.errorType = errorType;
		this.unexpectedObject = unexpectedObject;
	}

	public int getErrorType() {
		return errorType;
	}

	/**
	 * @return The character position (starting with 0) of the input where the
	 *         error occurs.
	 */
	public int getPosition() {
		return position;
	}

	/**
	 * @return One of the following base on the value of errorType:
	 *         ERROR_UNEXPECTED_CHAR java.lang.Character ERROR_UNEXPECTED_TOKEN
	 *         ERROR_UNEXPECTED_EXCEPTION java.lang.Exception
	 */
	public Object getUnexpectedObject() {
		return unexpectedObject;
	}

	public String toString() {
		return getMessage();
	}

	private static String toMessage(int position, int errorType, Object unexpectedObject) {
		StringBuilder sb = new StringBuilder();

		if (errorType == ERROR_UNEXPECTED_CHAR) {
			sb.append("Unexpected character (");
			sb.append(unexpectedObject);
			sb.append(") at position ");
			sb.append(position);
			sb.append(".");
		} else if (errorType == ERROR_UNEXPECTED_TOKEN) {
			sb.append("Unexpected token ");
			sb.append(unexpectedObject);
			sb.append(" at position ");
			sb.append(position);
			sb.append(".");
		} else if (errorType == ERROR_UNEXPECTED_EOF) {
			sb.append("Unexpected End Of File position ");
			sb.append(position);
			sb.append(": ");
			sb.append(unexpectedObject);
		} else if (errorType == ERROR_UNEXPECTED_UNICODE) {
			sb.append("Unexpected unicode escape secance ");
			sb.append(unexpectedObject);
			sb.append(" at position ");
			sb.append(position);
			sb.append(".");
		} else {
			sb.append("Unkown error at position ");
			sb.append(position);
			sb.append(".");
		}
		// sb.append("Unexpected exception at position ").append(position).append(": ").append(unexpectedObject);
		// break;
		return sb.toString();
	}
}
