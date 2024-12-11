package net.staticstudios.data;

public class DataDoesNotExistException extends RuntimeException {
    public DataDoesNotExistException(String message) {
        super(message);
    }
}
