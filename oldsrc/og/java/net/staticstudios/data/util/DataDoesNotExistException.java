package net.staticstudios.data.util;

public class DataDoesNotExistException extends RuntimeException {
    public DataDoesNotExistException(String message) {
        super(message);
    }
}
