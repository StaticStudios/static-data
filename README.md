# static-data

`static-data` is an ORM library originally designed for Minecraft servers. It provides a
robust solution for managing database operations in distributed applications while avoiding blocking operations at
runtime.
Minecraft servers are generally single threaded, so this library was built to avoid blocking the main thread during
database operations.
The main idea is simple: keep an in-memory cache of relevant database tables, and update that cache whenever the source
database changes.
As for the implementation, the in-memory cache is built using an in-memory H2 database, while PostgreSQL is used as the
source database. The source database is limited to PostgreSQL due to its support for the `LISTEN / NOTIFY` commands,
which
allow `static-data` to receive notifications whenever a change is made to the database.

## Key Features

### In-Memory Database with PostgreSQL Backend

`static-data` maintains a copy of relevant source tables in memory.
Only the tables that are needed by the application are kept in memory, not the entire database.
If two distinct applications use `static-data` with the same data source, their in-memory
caches will differ based on the tables they use. This design offers several advantages:

- Prevents blocking the current thread during database operations. I/O operations are preformed asynchronously.
- Provides instant reads and writes. These operations are preformed on the embedded in-memory database.
- The source database is updated in the background, using a FIFO queue to dispatch updates.

### Built for Distributed Applications

What makes `static-data` special is its ability to keep the in-memory cache updated whenever the source database
changes:

- When one application instance makes a change, all other instances update their cache quickly. (The delay comes from
  the latency from the application to the database)
- Prevents reading stale data in distributed environments.
- Simple developer API, `static-data` handles the complexity of keeping caches in sync.

### Comprehensive Relational Model Support

`static-data` supports a wide range of relational database features:

- One-to-one relationships
- One-to-many relationships
- Many-to-many relationships
- Foreign key constraints
- Custom indexes
- Default values

### PostgreSQL Integration

This ORM exclusively supports PostgreSQL as its source database:

- Uses PostgreSQL's `LISTEN / NOTIFY` commands to receive updates. This reduces complexity since there is no need to for
  an additional pub/sub service.
- Interoperates with other ORMs (like Hibernate) that might be used in other parts of your ecosystem. Whenever a change
  is made to the database, `static-data` will receive a notification and update its cache accordingly, there's no need
  to change other applications using the same datasource.

### Redis Support

`static-data` also supports using Redis as a data source for simple values:

- Works with primitive types and complex types with custom `ValueSerializer`s.
- Ideal for when persistence isn't a primary concern.

## Annotations and Data Wrappers

`static-data` v3 uses a combination of annotations and wrapper classes to define data models.
The annotations are primarily used for schema definition, while the wrapper classes are used to access and manipulate
data. There is no concept of "updating" a piece of data after a change is made.
Once a data wrapper's set (or other mutating method) is called, the change is immediately reflected in the
in-memory database and queued for writing to the source database.
The goal is to make the developer experience as seamless as possible.

### Annotations

- `@Data(schema = "...", table = "...")`: Defines the schema and table for a data class. Only applicable to classes
  extending `UniqueData`.
- `@IdColumn(name = "...")`: Marks a field as the ID column. There is support for multiple ID columns.
- `@Column(name = "...", nullable = true/false, index = true/false)`: Define a column in the current table.
- `@ForeignColumn(name = "...", table = "...", link = "...")`: Define a column in another table, and create a foreign
  key accordingly.
- `@Identifier([identifier])`: Specifies the identifier to use for `CachedValue<T>` fields. This is used in conjunction
  with the `UniqueData` instance's IDs to create a unique key in Redis.
- `@ExpireAfter([seconds])`: Used on `CachedValue<T>` fields to specify the expiration time in seconds. A value of 0
  means no expiration. When a value has expired, subsequent calls to `get()` will return `null`, or the fallback value
  if one is specified.
- `@OneToOne(link = "...")`: Defines a one-to-one relationship for `Reference<T>` fields. A foreign key constraint is
  created accordingly.
- `@OneToMany(link = "...")`: Defines a one-to-many relationship for `PersistentCollection<T>` fields. A foreign key
  constraint is created accordingly.
- `@ManyToMany(link = "...", joinTable = "...")`: Defines a many-to-many relationship for `PersistentCollection<T>`
  fields. A join table is created accordingly, and the appropriate foreign keys are created.
- `@DefaultValue("...")`: Sets a default value for a column. Note that this is a database-level default, not a
  Java-level default. Only Strings are supported, and they must be valid SQL literals. For example, for an integer
  column, you would use `@DefaultValue("0")`.
- `@Insert([InsertStrategy.PREFER_EXISTING/OVERWRITE_EXISTING])`: Controls insert behavior for `Reference<T>` and
  foreign
  columns.
- `@Delete([DeleteStrategy.CASCADE/NO_ACTION])`: Controls delete behavior. This has different behavior depending on the
  relationship type, refer to the javadoc on each `DeleteStrategy` enum value for more information.
- `@UpdateInterval([milliseconds])`: Used on `PersistentValue<T>` fields to control how often changes are flushed to the
  source database. The default is 0 milliseconds. Since a FIFO queue (one connection to the source database) is used to
  dispatch updates, frequent updates may clog up the queue. When the update interval is set to a non-zero value, only
  the latest change within the interval is queued for writing to the source database.

### Data Wrappers

- `PersistentValue<T>`: References a column in a table. Requires one of the annotations: `@IdColumn`, `@Column`, or
  `@ForeignColumn`.
- `CachedValue<T>`: References a value in redis, "linked" to a specific UniqueData instance. Requires `@Identifier`
  annotation.
- `Reference<T>`: References another data object (one-to-one relationship). Requires the `@OneToOne` annotation.
- `PersistentCollection<T>`: Represents a collection relationship (one-to-many or many-to-many). One to many
  collections support value types that do not extend `UniqueData`, such as `PersistentCollection<String>`. Requires
  either the`@OneToMany` or`@ManyToMany` annotation.

### Compile-time Generated Classes

For each data class, `static-data` generates two helper classes at compile time, for typesafe operations:

1. **Builder**: Provides a builder pattern for creating and inserting instances
   ```
   // Example: Creating and inserting a new user
   User user = User.builder()
       .id(UUID.randomUUID())
       .name("John Doe")
       .age(30)
       .insert(InsertMode.ASYNC);
   ```

2. **Query Builder**: Provides a fluent API for querying instances
   ```
   // Example: Finding users by criteria
   List<User> users = User.query()
       .where(w -> w
           .nameIsLike("John%")
           .and()
           .ageIsGreaterThan(25)
       )
       .orderByName(Order.ASCENDING)
       .limit(10)
       .findAll();
   ```

Note: Currently only support for Intellij IDEA is available for IDE integration. You should install the appropriate
plugin for your IDE.

## Data Types

Any class can be used as a data type, provided it's a "Primitive" or has a registered `ValueSerializer`. "Primitive"
types are basic types supported in PostgreSQL:

- `String`, `Integer`, `Long`, `Float`, `Double`, `Boolean`, `UUID`, and `Timestamp`.

Nullability is controlled through the `nullable` parameter in the `@Column` and `@ForeignColumn` annotations. For
example:

```
@Column(name = "age", nullable = true)
public PersistentValue<Integer> age;
```

This flexibility allows all primitive types to be nullable when needed, while still maintaining type safety.

## Usage Example

[//]: # (TODO: validate the create method calls and ensure theyre correct, i did thie from memory)

[//]: # ()

[//]: # (```java)

[//]: # ()

[//]: # (@Data&#40;schema = "my_app", table = "users"&#41;)

[//]: # (public class User extends UniqueData {)

[//]: # (    @IdColumn&#40;name = "id"&#41;)

[//]: # (    private PersistentValue<Integer> id;)

[//]: # ()

[//]: # (    @OneToOne&#40;link = "id=user_id"&#41;)

[//]: # (    private Reference<UserMetadata> metadata;)

[//]: # ()

[//]: # (    @Column&#40;name = "name", index = true, nullable = false&#41;)

[//]: # (    private PersistentValue<String> name;)

[//]: # ()

[//]: # (    @ManyToMany&#40;link = "id=id", joinTable = "user_friends"&#41;)

[//]: # (    private PersistentCollection<User> friends;)

[//]: # ()

[//]: # (    /**)

[//]: # (     * Create a new user and their metadata in one transaction.)

[//]: # (     * @param id the user ID)

[//]: # (     * @return the created user)

[//]: # (     */)

[//]: # (    public static User create&#40;int id&#41; {)

[//]: # (        InsertContext ctx = DataManager.getInstance&#40;&#41;.createInsertContext&#40;&#41;;)

[//]: # (        UserMetadataFactory.builder&#40;&#41;)

[//]: # (                .id&#40;UUID.randomUUID&#40;&#41;&#41;)

[//]: # (                .userId&#40;id&#41;)

[//]: # (                .createdAt&#40;new Timestamp&#40;System.currentTimeMillis&#40;&#41;&#41;&#41;)

[//]: # (                .insert&#40;InsertMode.SYNC, ctx&#41;;)

[//]: # (        UserFactory factory = UserFactory.builder&#40;&#41;)

[//]: # (                .id&#40;id&#41;)

[//]: # (                .name&#40;"User #" + id&#41;)

[//]: # (                .insert&#40;InsertMode.SYNC, ctx&#41;;)

[//]: # ()

[//]: # (        ctx.insert&#40;&#41;;)

[//]: # (        return factory.get&#40;User.class&#41;;)

[//]: # (    })

[//]: # ()

[//]: # (    public int getId&#40;&#41; {)

[//]: # (        return id.get&#40;&#41;;)

[//]: # (    })

[//]: # ()

[//]: # (    public void setId&#40;int id&#41; {)

[//]: # (        this.id.set&#40;id&#41;;)

[//]: # (    })

[//]: # ()

[//]: # (    public String getName&#40;&#41; {)

[//]: # (        return name.get&#40;&#41;;)

[//]: # (    })

[//]: # ()

[//]: # (    public void setName&#40;String name&#41; {)

[//]: # (        this.name.set&#40;name&#41;;)

[//]: # (    })

[//]: # ()

[//]: # (    public UserMetadata getMetadata&#40;&#41; {)

[//]: # (        return metadata.get&#40;&#41;;)

[//]: # (    })

[//]: # ()

[//]: # (    public void setMetadata&#40;UserMetadata metadata&#41; {)

[//]: # (        this.metadata.set&#40;metadata&#41;;)

[//]: # (    })

[//]: # ()

[//]: # (    public Collection<User> getFriends&#40;&#41; {)

[//]: # (        return friends.get&#40;&#41;;)

[//]: # (    })

[//]: # ()

[//]: # (    public void addFriend&#40;User friend&#41; {)

[//]: # (        this.friends.add&#40;friend&#41;;)

[//]: # (    })

[//]: # ()

[//]: # (    public void removeFriend&#40;User friend&#41; {)

[//]: # (        this.friends.remove&#40;friend&#41;;)

[//]: # (    })

[//]: # (})

[//]: # ()

[//]: # (@Data&#40;schema = "my_app", table = "user_metadata"&#41;)

[//]: # (public class UserMetadata extends UniqueData {)

[//]: # (    @IdColumn&#40;name = "id"&#41;)

[//]: # (    private PersistentValue<UUID> id;)

[//]: # ()

[//]: # (    @OneToOne&#40;link = "id=id"&#41;)

[//]: # (    private Reference<User> user;)

[//]: # ()

[//]: # (    @Column&#40;name = "user_id", index = true, nullable = false&#41;)

[//]: # (    private PersistentValue<Integer> userId;)

[//]: # ()

[//]: # (    @Column&#40;name = "created_at", nullable = false&#41;)

[//]: # (    private PersistentValue<Timestamp> createdAt;)

[//]: # ()

[//]: # (    public UUID getId&#40;&#41; {)

[//]: # (        return id.get&#40;&#41;;)

[//]: # (    })

[//]: # ()

[//]: # (    public void setId&#40;UUID id&#41; {)

[//]: # (        this.id.set&#40;id&#41;;)

[//]: # (    })

[//]: # ()

[//]: # (    public User getUser&#40;&#41; {)

[//]: # (        return user.get&#40;&#41;;)

[//]: # (    })

[//]: # ()

[//]: # (    public void setUser&#40;User user&#41; {)

[//]: # (        this.user.set&#40;user&#41;;)

[//]: # (    })

[//]: # ()

[//]: # (    public int getUserId&#40;&#41; {)

[//]: # (        return userId.get&#40;&#41;;)

[//]: # (    })

[//]: # ()

[//]: # (    public void setUserId&#40;int userId&#41; {)

[//]: # (        this.userId.set&#40;userId&#41;;)

[//]: # (    })

[//]: # ()

[//]: # (    public Timestamp getCreatedAt&#40;&#41; {)

[//]: # (        return createdAt.get&#40;&#41;;)

[//]: # (    })

[//]: # ()

[//]: # (    public void setCreatedAt&#40;Timestamp createdAt&#41; {)

[//]: # (        this.createdAt.set&#40;createdAt&#41;;)

[//]: # (    })

[//]: # (})

[//]: # ()

[//]: # (```)

## Current Limitations

- Memory usage: While the whole database is not kept in memory, only relevant tables are, this can still use significant
  amounts of memory for large tables.

## Future Developments

- **PostgreSQL-only mode**: A future update will add support for a PostgreSQL-only mode where `static-data` will act as
  a traditional ORM without using an in-memory cache. This will provide better performance for applications that don't
  need the caching benefits and will reduce memory usage, while still providing the same developer experience.

- **Disk-based cache**: Plans are in place to add support for a disk-based cache option (using H2 on disk instead of in
  memory) to reduce memory consumption while still maintaining the benefits of the caching architecture.

## Miscellaneous

- Anywhere a schema, table, or column name can be specified, environment variables can be used via the syntax
  `${ENV_VAR_NAME}`.
  This allows for dynamic configuration based on the deployment environment.

[//]: # (## Getting Started)

[//]: # (TODO: this section is incomplete since the impl isnt finished. update this later)

[//]: # (TODO: talk about update handlers, & add/remove handlers)