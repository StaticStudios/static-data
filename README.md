# static-data

`static-data` is an ORM (Object-Relational Mapping) library primarily designed for Minecraft servers. It provides a
robust solution for managing database operations in distributed applications while avoiding blocking the main thread.

## Key Features

### In-Memory Database with PostgreSQL Backend

`static-data` maintains a copy of the source PostgreSQL database in memory as an H2 database. This architecture:

- Prevents blocking the main thread during database operations
- Provides fast read access to data
- Asynchronously dispatches writes to the source database

### Built for Distributed Applications

What makes `static-data` special is its ability to keep the in-memory cache updated whenever the source database
changes:

- When one application instance makes a change, all other instances update their cache
- Prevents reading stale data in distributed environments
- Different application instances can track different subsets of data

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

- Uses PostgreSQL's `LISTEN / NOTIFY` commands to receive updates
- Avoids adding additional layers between the application and the database
- Interoperates with other ORMs (like Hibernate) that might be used in other parts of your ecosystem

### Redis Support

`static-data` also supports using Redis as a data source for simple values:

- Works with primitive types and complex types with custom `ValueSerializer`s
- Ideal for when persistence isn't a primary concern

## Annotations and Data Wrappers

`static-data` v3 uses a combination of annotations and wrapper classes to define data models:

### Annotations

- `@Data(schema = "...", table = "...")`: Defines the schema and table for a data class
- `@IdColumn(name = "...")`: Marks a field as the ID column
- `@Column(name = "...", nullable = true/false, index = true/false)`: Maps a field to a database column
- `@ForeignColumn(name = "...", table = "...", link = "...")`: Maps a field to a column in a different table
- `@OneToOne(link = "...")`: Defines a one-to-one relationship
- `@OneToMany(link = "...")`: Defines a one-to-many relationship
- `@ManyToMany(link = "...", joinTable = "...")`: Defines a many-to-many relationship
- `@DefaultValue("...")`: Sets a default value for a column
- `@Insert(InsertStrategy.PREFER_EXISTING/OVERWRITE_EXISTING)`: Controls insert behavior
- `@Delete(DeleteStrategy.CASCADE/NO_ACTION)`: Controls delete behavior
- `@UpdateInterval(milliseconds)`: Sets an interval for batching updates

### Data Wrappers

- `PersistentValue<T>`: References a column in a data object's row
- `Reference<T>`: References another data object (one-to-one relationship)
- `PersistentCollection<T>`: Represents a collection relationship (one-to-many or many-to-many)

### Compile-time Generated Classes

For each data class, `static-data` generates two helper classes at compile time:

1. **Factory**: Provides a builder pattern for creating and inserting instances
   ```
   // Example: Creating and inserting a new user
   User user = UserFactory.builder(dataManager)
       .id(UUID.randomUUID())
       .name("John Doe")
       .age(30)
       .insert(InsertMode.SYNC);
   ```

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

```
@Data(schema = "social_media", table = "posts")
public class Post extends UniqueData {
    @IdColumn(name = "post_id")
    public PersistentValue<Integer> id;

    @OneToOne(link = "post_id=metadata_id")
    public Reference<PostMetadata> metadata;

    @Column(name = "text_content", index = true)
    public PersistentValue<String> textContent;

    @DefaultValue("0")
    @Column(name = "likes")
    public PersistentValue<Integer> likes;

    @ManyToMany(link = "post_id=post_id", joinTable = "posts_related")
    public PersistentCollection<Post> relatedPosts;
}
```

## Current Limitations

- Memory usage: While the whole database is not kept in memory, only relevant tables are, this can still use significant
  amounts of memory for large tables.

## Future Developments

- **PostgreSQL-only mode**: A future update will add support for a PostgreSQL-only mode where `static-data` will act as
  a traditional ORM without using an in-memory cache. This will provide better performance for applications that don't
  need the caching benefits and will reduce memory usage.

- **Disk-based cache**: Plans are in place to add support for a disk-based cache option (using H2 on disk instead of in
  memory) to reduce memory consumption while still maintaining the benefits of the caching architecture.

## Getting Started

[//]: # (TODO: this section is incorrect since the impl isnt finished. update this later)

1. Configure your data source:

```
DataSourceConfig config = new DataSourceConfig.Builder()
    .setPostgresUrl("jdbc:postgresql://localhost:5432/mydatabase")
    .setPostgresUsername("username")
    .setPostgresPassword("password")
    .build();
```

2. Initialize the DataManager:

```
DataManager dataManager = new DataManager(config);
```

3. Define your data models using annotations and data wrappers.

4. Load your models:

```
dataManager.load(Post.class, User.class);
```

5. Query and manipulate data:

```
// Get a post by ID
Post post = dataManager.getInstance(Post.class, new ColumnValuePair("post_id", 1));

// Update a value
post.likes.set(post.likes.get() + 1);

// Access related objects
PostMetadata metadata = post.metadata.get();
```
