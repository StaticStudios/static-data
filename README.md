# This is not ready for public use
- Documentation needs to be completed
- Single point of failures need to be addressed

# What is static-data
`static-data` is an ORM built for a specific type of application. Origninally created for distributed Minecraft servers, this ORM avoids blocking an application's main thread when doing read or writes from a remote datasource. This is accomplished by keeping an in memory copy of the datasource, reading from there, and asynchronously disbatching writes to the datasource. The whole database is **not** kept in memory, only relevant tables are. This can use significant amounts of memory on large tables.

## Built for distributed applications
What makes `static-data` special is that the in-memory cache is updated whenever the datasource is updated. This means that when one application instance makes a change, all other instances will update their cache. This avoids reading stale data. The various instaces do not have to contain the exact same data classes either, `static-data` sends updates based off of updated cells in the database. So if a cell is being tracked somewhere, the update will be received.

### PostgreSQL only
This ORM only supports PostgreSQL. The reason is that `static-data` makes use of PostgreSQL's `LISTEN / NOTIFY` commands to recieve updates rather than add an additional layer between the application and the database.

`static-data` was designed to interop with other ORM's such as Hibernate. Not nessicarily within the same application, but if a secondary applicaition (that uses the same tables) such as a SpringBoot API is created, `static-data` does not have to be used there. Updates made with Hibernate will still be propogated to all application instances using `static-data`, since this is done on the database level.

### Redis support
`static-data` supports using redis as a data source for simple values. Simple just means no collections/references. "Primative" types and complex types with custom `ValueSerializer`s are supported here.

## Data wrappers
- `PersistentValue.of(...)`: reference a column in a data object's row
- `PersistentValue.foreign(...)`: reference a column in a different table
- `CachedValue.of(...)`: reference an entry in redis (used when persistence doesn't matter)
- `Reference.of(...)`: reference another data object (one-to-one relationship)
- `PersistentCollection.of(...)`: represents a one-to-many relationship of simple data types such as Integer, Boolean, etc.. (like a `PersistentValue`, but one-to-many)
- `PersistentCollection.oneToMany(...)`: represents a one-to-many relationship of other data objects (like a `Reference`, but one-to-many)
- `PersistentCollection.manyToMany(...)`: represents a many-to-many relationship of other data objects, with the use of a junction table

# Current limitations
Currently, `static-data` assumes that any column marked as an id column will not have its value changed. It should only ever be `null` when the row/data object doesn't exist. Changing this value will break things in many ways.

