# static-data

`static-data` is an ORM (Object-Relational Mapping) library originally designed for Minecraft servers. It provides a
robust solution for managing database operations in distributed applications while avoiding blocking the main thread.
This is what makes it different from other ORMs, read and write speed is the main focus.

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
- `@OneToOne(link = "...")`: Defines a one-to-one relationship for `Reference<T>` fields. A foreign key constraint is
  created accordingly.
- `@OneToMany(link = "...")`: Defines a one-to-many relationship for `PersistentCollection<T>` fields. A foreign key
  constraint is created accordingly.
- `@ManyToMany(link = "...", joinTable = "...")`: Defines a many-to-many relationship for `PersistentCollection<T>`
  fields. A join table is created accordingly, and the appropriate foreign keys are created.
- `@DefaultValue("...")`: Sets a default value for a column. Note that this is a database-level default, not a
  Java-level default. Only Strings are supported, and they must be valid SQL literals. For example, for an integer
  column, you would use `@DefaultValue("0")`.
- `@Insert(InsertStrategy.PREFER_EXISTING/OVERWRITE_EXISTING)`: Controls insert behavior for `Reference<T>` and foreign
  columns.
- `@Delete(DeleteStrategy.CASCADE/NO_ACTION)`: Controls delete behavior. This has different behavior depending on the
  relationship type, refer to the javadoc on each `DeleteStrategy` enum value for more information.
- `@UpdateInterval(milliseconds)`: Used on `PersistentValue<T>` fields to control how often changes are flushed to the
  source database. The default is 0 milliseconds. Since a FIFO queue (one connection to the source database) is used to
  dispatch updates, frequent updates may clog up the queue. When the update interval is set to a non-zero value, only
  the latest change within the interval is queued for writing to the source database.

### Data Wrappers

- `PersistentValue<T>`: References a column in a table. Requires one of the annotations: `@IdColumn`, `@Column`, or
  `@ForeignColumn`.
- `Reference<T>`: References another data object (one-to-one relationship). Requires the `@OneToOne` annotation.
- `PersistentCollection<T>`: Represents a collection relationship (one-to-many or many-to-many). Requires either the
  `@OneToMany` or
  `@ManyToMany` annotation.

### Compile-time Generated Classes

For each data class, `static-data` generates two helper classes at compile time, for typesafe operations:

1. **Factory**: Provides a builder pattern for creating and inserting instances
   ```
   // Example: Creating and inserting a new user
   User user = UserFactory.builder(dataManager)
       .id(UUID.randomUUID())
       .name("John Doe")
       .age(30)
       .insert(InsertMode.SYNC);
   ```

Note: The factory can use the global singleton DataManager instance if not explicitly provided.
In the above example, `UserFactory.builder(dataManager)` can be replaced with `UserFactory.builder()` if the global
instance is set.

2. **Query Builder**: Provides a fluent API for querying instances
   ```
   // Example: Finding users by criteria
   List<User> users = UserQuery.where(dataManager)
       .nameIsLike("John%")
       .and()
       .ageIsGreaterThan(25)
       .orderByName(Order.ASC)
       .limit(10)
       .list();
   ```

Note: The query builder can use the global singleton DataManager instance if not explicitly provided.
In the above example, `UserQuery.where(dataManager)` can be replaced with `UserQuery.where()` if the global instance
is set.

## Data Types

Any class can be used as a data type, provided it's a "Primitive" or has a registered `ValueSerializer`. "Primitive"
types are basic types supported in PostgreSQL:

- `String`, `Integer`, `Long`, `Float`, `Double`, `Boolean`, `UUID`, `Timestamp`, and `byte[]`

Nullability is controlled through the `nullable` parameter in the `@Column` and `@ForeignColumn` annotations. For
example:

```
@Column(name = "age", nullable = true)
public PersistentValue<Integer> age;
```

This flexibility allows all primitive types to be nullable when needed, while still maintaining type safety.

## Usage Example

[//]: # (TODO: validate the create method calls and ensure theyre correct, i did thie from memory)

```java

@Data(schema = "my_app", table = "users")
public class User extends UniqueData {
    @IdColumn(name = "id")
    private PersistentValue<Integer> id;

    @OneToOne(link = "id=user_id")
    private Reference<UserMetadata> metadata;

    @Column(name = "name", index = true, nullable = false)
    private PersistentValue<String> name;

    @ManyToMany(link = "id=id", joinTable = "user_friends")
    private PersistentCollection<User> friends;

    /**
     * Create a new user and their metadata in one transaction.
     * @param id the user ID
     * @return the created user
     */
    public static User create(int id) {
        InsertContext ctx = DataManager.getInstance().createInsertContext();
        UserMetadataFactory.builder()
                .id(UUID.randomUUID())
                .userId(id)
                .createdAt(new Timestamp(System.currentTimeMillis()))
                .insert(InsertMode.SYNC, ctx);
        UserFactory factory = UserFactory.builder()
                .id(id)
                .name("User #" + id)
                .insert(InsertMode.SYNC, ctx);

        ctx.insert();
        return factory.get(User.class);
    }

    public int getId() {
        return id.get();
    }

    public void setId(int id) {
        this.id.set(id);
    }

    public String getName() {
        return name.get();
    }

    public void setName(String name) {
        this.name.set(name);
    }

    public UserMetadata getMetadata() {
        return metadata.get();
    }

    public void setMetadata(UserMetadata metadata) {
        this.metadata.set(metadata);
    }

    public Collection<User> getFriends() {
        return friends.get();
    }

    public void addFriend(User friend) {
        this.friends.add(friend);
    }

    public void removeFriend(User friend) {
        this.friends.remove(friend);
    }
}

@Data(schema = "my_app", table = "user_metadata")
public class UserMetadata extends UniqueData {
    @IdColumn(name = "id")
    private PersistentValue<UUID> id;

    @OneToOne(link = "id=id")
    private Reference<User> user;

    @Column(name = "user_id", index = true, nullable = false)
    private PersistentValue<Integer> userId;

    @Column(name = "created_at", nullable = false)
    private PersistentValue<Timestamp> createdAt;

    public UUID getId() {
        return id.get();
    }

    public void setId(UUID id) {
        this.id.set(id);
    }

    public User getUser() {
        return user.get();
    }

    public void setUser(User user) {
        this.user.set(user);
    }

    public int getUserId() {
        return userId.get();
    }

    public void setUserId(int userId) {
        this.userId.set(userId);
    }

    public Timestamp getCreatedAt() {
        return createdAt.get();
    }

    public void setCreatedAt(Timestamp createdAt) {
        this.createdAt.set(createdAt);
    }
}

```

## Current Limitations

- Memory usage: While the whole database is not kept in memory, only relevant tables are, this can still use significant
  amounts of memory for large tables.

## Future Developments

- **PostgreSQL-only mode**: A future update will add support for a PostgreSQL-only mode where `static-data` will act as
  a traditional ORM without using an in-memory cache. This will provide better performance for applications that don't
  need the caching benefits and will reduce memory usage, while still providing the same developer experience.

- **Disk-based cache**: Plans are in place to add support for a disk-based cache option (using H2 on disk instead of in
  memory) to reduce memory consumption while still maintaining the benefits of the caching architecture.

## Getting Started

[//]: # (TODO: this section is incomplete since the impl isnt finished. update this later)

[//]: # (TODO: talk about update handlers, & add/remove handlers)