package net.staticstudios.data.util;

public class PostgresUtils {
    public static byte[] toBytes(String hex) {
        hex = hex.substring(2); // Remove the \x prefix
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < hex.length(); i += 2) {
            bytes[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4) + Character.digit(hex.charAt(i + 1), 16));
        }

        return bytes;
    }

    public static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder("\\x");
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }

        return sb.toString();
    }
}
