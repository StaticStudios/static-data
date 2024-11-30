# Why does this exist
`static-data` exists for use in distributed Minecraft servers, where additional latency on read operations cannot be afforded.

## Data wrappers
A data wrapper is meant to be a field in a `UniqueData` object. Currently, the following data wrappers have been implemented:
- `PersistentValue`: This value is stored in the database. It is referenced via the `@Table` annotation on the parent data class, and by it's column which is explicitly defined.
- `ForeignPersistentValue`: This is similar to `PersistentValue`, however it does not use the `@Table` annotation. Instead, the table must be explicitly defined. Furthermore, it references a column in a different data object, hence the name foreign.
- `CachedValue`: This is a value stored in Redis. It will fallback to its default value if the value is not present in redis.
- `PersistentCollection`: This is a collection of values that is stored in the database. The current implementation is a list based implementation.
