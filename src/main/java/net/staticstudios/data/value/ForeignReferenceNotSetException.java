package net.staticstudios.data.value;

public class ForeignReferenceNotSetException extends RuntimeException {
    public ForeignReferenceNotSetException(String message) {
        super(message);
    }
}
