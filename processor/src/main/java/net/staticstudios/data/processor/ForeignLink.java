package net.staticstudios.data.processor;

public record ForeignLink(String foreignColumnFieldName, PersistentValueMetadata localColumnMetadata) {
}
