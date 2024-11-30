package net.staticstudios.data.value;

public class ForeignReferenceDoesNotExistException extends RuntimeException {
    public ForeignReferenceDoesNotExistException(String message) {
        super(message);
    }
}
