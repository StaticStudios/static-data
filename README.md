# static-data

`static-data` is a Java 21 ORM and distributed in-memory data layer built for applications that cannot afford to wait on a remote database during normal request or game-loop processing.

PostgreSQL remains the source of truth, but every table used by the application is copied into an embedded H2 database at startup. Reads, queries, and local mutations use H2. Changes are then sent to PostgreSQL through a single background FIFO queue, while PostgreSQL triggers and `LISTEN`/`NOTIFY` keep other `static-data` instances up to date. Redis provides the equivalent mechanism for `CachedValue` fields.

This is an intentionally opinionated design. It is useful for Minecraft servers and other latency-sensitive, distributed applications, but it is not a drop-in replacement for a traditional request-oriented ORM. Read [Consistency and write behavior](#consistency-and-write-behavior) and [Current limitations](#current-limitations) before adopting it.

## How it works

1. Annotated Java classes describe tables, columns, and relationships.
2. The annotation processor adds type-safe builders and query builders during compilation.
3. `StaticData.load(...)` creates missing database objects and registers the tables used by the application.
4. `StaticData.finishLoading()` copies those PostgreSQL tables into the in-memory H2 database and discovers matching Redis values.
5. Queries read from H2. Mutations update H2 immediately and are normally propagated to PostgreSQL or Redis in the background.
6. Database triggers notify every other running instance, which applies the change to its own H2 cache.

Only loaded tables are copied; `static-data` does not mirror the entire PostgreSQL database. Loading one model also loads models referenced by its relationship fields.

## Requirements

- Java 21
- PostgreSQL
- Redis, even if the first version of your model does not use `CachedValue`
- Annotation processing enabled for the build
- A PostgreSQL user allowed to create the schemas, tables, columns, indexes, functions, and triggers described by the models
- Redis keyevent notifications enabled

Enable the Redis notifications used for cache synchronization in `redis.conf`:

```text
notify-keyspace-events KEA
```

For local development, the equivalent runtime command is:

```shell
redis-cli CONFIG SET notify-keyspace-events KEA
```

## Installation

The current project version is `3.3.12-SNAPSHOT`. With Gradle (Groovy DSL):

```groovy
repositories {
    mavenCentral()
    maven {
        url = uri("https://repo.staticstudios.net/snapshots/")
    }
}

dependencies {
    implementation "net.staticstudios:static-data:3.3.12-SNAPSHOT"
    annotationProcessor "net.staticstudios:static-data-processor:3.3.12-SNAPSHOT"
}
```

The processor modifies the compiled class to add `builder()` and `query()` APIs. IntelliJ IDEA support is available through the `static-data` IntelliJ plugin in this repository. Other IDEs may report false errors for those generated methods even when the Gradle build succeeds.

## Quick start

### 1. Define a model

Every persistent model extends `UniqueData`, has a `@Data` annotation, and declares at least one `@IdColumn`. The class must also have a no-argument constructor; the implicit constructor is sufficient.

```java
package com.example.data;

import net.staticstudios.data.Column;
import net.staticstudios.data.Data;
import net.staticstudios.data.IdColumn;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.UniqueData;

import java.util.UUID;

@Data(schema = "my_app", table = "users")
public final class User extends UniqueData {
    @IdColumn(name = "id")
    private PersistentValue<UUID> id;

    @Column(name = "name", index = true)
    private PersistentValue<String> name;

    @Column(name = "age", nullable = true)
    private PersistentValue<Integer> age;

    public UUID id() {
        return id.get();
    }

    public String name() {
        return name.get();
    }

    public void rename(String newName) {
        name.set(newName);
    }
}
```

The wrapper fields are initialized by `static-data` when an instance is created. Initialize one explicitly only when configuring behavior such as a fallback, refresher, or update handler.

### 2. Initialize and load the ORM

Initialization is a startup operation. It opens PostgreSQL and Redis connections and installs the PostgreSQL notification function. Loading executes additive DDL, and finishing the load copies all rows from the registered PostgreSQL tables into H2.

```java
import net.staticstudios.data.StaticData;
import net.staticstudios.data.StaticDataConfig;

StaticDataConfig config = StaticDataConfig.builder()
        .postgresHost("127.0.0.1")
        .postgresPort(5432)
        .postgresDatabase("my_database")
        .postgresUsername("my_user")
        .postgresPassword("my_password")
        .redisHost("127.0.0.1")
        .redisPort(6379)
        .build();

StaticData.init(config);

// Register custom ValueSerializers here, before loading models.
StaticData.load(User.class);
StaticData.finishLoading();
```

You may call `load(...)` more than once for distinct models before `finishLoading()`. Do not load a model again if it was already discovered through a relationship. After `finishLoading()` returns, the registered models are ready to use and no more models can be loaded into that `StaticData` instance.

`StaticDataConfig.updateHandlerExecutor(...)` controls where update and collection callbacks run. Its default uses the library's background thread provider. Applications whose callbacks must run on a particular thread, such as a Minecraft server's main thread, should supply the appropriate scheduler.

### 3. Insert, query, update, and delete

The annotation processor derives builder methods and query predicates from the Java field names:

```java
import net.staticstudios.data.InsertMode;
import net.staticstudios.data.Order;

import java.util.List;
import java.util.UUID;

User inserted = User.builder()
        .id(UUID.randomUUID())
        .name("Ada")
        .age(31)
        .insert(InsertMode.ASYNC);

User ada = User.query()
        .where(w -> w.nameIs("Ada"))
        .findOne();

List<User> adults = User.query()
        .where(w -> w.ageIsGreaterThanOrEqualTo(18))
        .orderByName(Order.ASCENDING)
        .findAll();

if (ada != null) {
    ada.rename("Ada Lovelace");
    ada.delete();
}
```

`findOne()` returns `null` when nothing matches. `findAll()` returns a list. Generated predicates include equality, inequality, `IN`, null checks, string matching, and range comparisons where the field type supports them. Queries operate on the local H2 copy, so they do not perform PostgreSQL network I/O.

Use `and()`, `or()`, and `group(...)` to compose conditions:

```java
List<User> users = User.query()
        .where(w -> w
                .nameIsLike("A%")
                .and()
                .group(g -> g.ageIsNull().or().ageIsGreaterThan(20)))
        .findAll();
```

## Data wrappers

Models describe storage through four wrapper types:

| Wrapper | Storage | Purpose |
| --- | --- | --- |
| `PersistentValue<T>` | PostgreSQL, mirrored in H2 | One table cell, annotated with `@IdColumn`, `@Column`, or `@ForeignColumn` |
| `Reference<T>` | PostgreSQL relationship | A one-to-one reference, annotated with `@OneToOne` |
| `PersistentCollection<T>` | PostgreSQL relationship or join table | A one-to-many or many-to-many collection |
| `CachedValue<T>` | Redis, mirrored in H2 as a virtual column | A temporary or fast-changing value associated with a `UniqueData` instance |

Calling `set`, `add`, `remove`, or `clear` changes the local representation immediately and schedules the source-store update. There is no separate entity `save()` operation.

### Annotation reference

The most commonly used model annotations are:

| Annotation | Applies to | Purpose |
| --- | --- | --- |
| `@Data(schema, table)` | A `UniqueData` class | Selects the model's PostgreSQL table |
| `@IdColumn(name)` | `PersistentValue<T>` | Defines one part of the primary key; multiple ID fields form a composite key |
| `@Column(name, nullable, index, unique)` | `PersistentValue<T>` | Defines a column in the model's own table |
| `@ForeignColumn(name, schema, table, link, ...)` | `PersistentValue<T>` | Maps a value stored in another table and links that row to the holder |
| `@DefaultValue(value)` | A persistent value field | Adds a raw database-level default expression |
| `@OneToOne(link, fkey, updateReferencedColumns)` | `Reference<T>` | Defines a one-to-one relationship |
| `@OneToMany(link, ...)` | `PersistentCollection<T>` | Defines a collection of models or plain values |
| `@ManyToMany(link, joinTable, joinTableSchema, fkey)` | `PersistentCollection<T>` | Defines a model collection through a join table |
| `@Identifier(value, index)` | `CachedValue<T>` | Defines the Redis key component and optional H2 virtual-column index |
| `@ExpireAfter(seconds)` | `CachedValue<T>` | Sets the Redis TTL; values at or below zero do not expire |
| `@UpdateInterval(milliseconds)` | `PersistentValue<T>` or `CachedValue<T>` | Coalesces frequent source-store writes |
| `@Insert(strategy)` | A foreign-table field | Chooses whether an insert preserves or overwrites an existing foreign row |
| `@Delete(strategy)` | A foreign-table or relationship field | Chooses how related data is handled when the holder is deleted |

Names inside these annotations support `${ENVIRONMENT_VARIABLE}` substitution.

## Update and collection handlers

Handlers are part of the model definition. Configure them in a wrapper's field initializer; they cannot be attached dynamically after the wrapper has been initialized. The first initialized live instance registers the field's handlers for every instance of that model, not only for the object whose constructor created the proxy.

### Persistent value updates

`PersistentValue.onUpdate(...)` receives the holder plus a `ValueUpdate<T>` containing the value before and after the committed H2 update:

```java
@Column(name = "name", index = true)
private PersistentValue<String> name = PersistentValue.of(this, String.class)
        .onUpdate(User.class, (user, update) ->
                user.handleNameChange(update.oldValue(), update.newValue()));

private void handleNameChange(String oldName, String newName) {
    System.out.printf("Name changed from %s to %s%n", oldName, newName);
}
```

The current implementation calls persistent-value handlers for actual column updates, not for the initial row insert. A handler runs for local mutations and for external PostgreSQL changes after they have been applied to H2.

Both values can be `null` when the column is nullable. Multiple `onUpdate(...)` calls may be chained if more than one reaction is needed.

### Reference updates

`Reference.onUpdate(...)` has the same old/new shape, but the values are related `UniqueData` instances:

```java
@Column(name = "best_friend_id", nullable = true, unique = true)
private PersistentValue<UUID> bestFriendId;

@OneToOne(link = "best_friend_id=id")
private Reference<User> bestFriend = Reference.of(this, User.class)
        .onUpdate(User.class, (user, update) -> {
            User oldFriend = update.oldValue();
            User newFriend = update.newValue();
            user.onBestFriendChanged(oldFriend, newFriend);
        });
```

`null -> value` means that a reference was established, `value -> null` means that it was removed, and two non-null values represent a replacement. Read with `bestFriend.get()` and change the relationship with `bestFriend.set(otherUser)` or `bestFriend.set(null)`.

### Collection add and remove handlers

`PersistentCollection` implements Java's `Collection` interface. Its `onAdd(...)` and `onRemove(...)` handlers receive the holder and the element whose membership changed:

```java
@OneToMany(link = "id=user_id")
@Delete(DeleteStrategy.CASCADE)
private PersistentCollection<UserSession> sessions =
        PersistentCollection.of(this, UserSession.class)
                .onAdd(User.class, (user, session) ->
                        user.onSessionAdded(session))
                .onRemove(User.class, (user, session) ->
                        user.onSessionRemoved(session));
```

```java
user.sessions.add(session);
user.sessions.remove(session);
user.sessions.addAll(newSessions);
user.sessions.clear();
```

These callbacks describe relationship membership, not only direct method calls. They also run when an external database change, a changed linking column, a join-row change, or a related deletion changes the collection after synchronization.

When deleting a related `UniqueData` row causes an `onRemove` callback, the removed element may be a read-only snapshot because the live row no longer exists. A one-to-many collection of plain values passes the removed value directly.

### Handler execution rules

- Handlers are submitted through `StaticDataConfig.updateHandlerExecutor(...)`; configure that executor if application state must be touched on a specific thread.
- The update has already committed to local H2 when the handler is submitted.
- Calling `load(...)` does not instantiate every model. A model's handlers become active after its first live instance has initialized the corresponding wrappers.
- Handler and refresher lambdas must not capture a `UniqueData` instance from their enclosing scope. Use the holder argument supplied to the callback.
- Prefer stateless handlers. Updating the same field or relationship from its own handler can create a callback loop.
- The callback API does not expose whether a change originated locally or from another application instance.

## Relationships

Relationship links use `localColumn=foreignColumn` syntax:

```java
@Data(schema = "my_app", table = "user_sessions")
public final class UserSession extends UniqueData {
    @IdColumn(name = "id")
    private PersistentValue<UUID> id;

    @Column(name = "user_id")
    private PersistentValue<UUID> userId;

    @OneToOne(link = "user_id=id")
    private Reference<User> user;
}
```

```java
// Added to User: User.id is matched to UserSession.userId.
@OneToMany(link = "id=user_id")
@Delete(DeleteStrategy.CASCADE)
private PersistentCollection<UserSession> sessions;
```

- `@OneToOne` maps a `Reference<T>`.
- `@OneToMany` maps a `PersistentCollection<T>`. Its element can be another `UniqueData` type or a supported value type.
- `@ManyToMany` maps a `PersistentCollection<T>` through a join table.
- `@Insert(PREFER_EXISTING | OVERWRITE_EXISTING)` controls how foreign-table values are treated during an insert.

For `@OneToMany` and `@OneToOne`, the left side of `link` belongs to the class containing the field. The right side belongs to the referenced table or class.

One-to-many collections can also store plain supported values in a dedicated table:

```java
@OneToMany(
        link = "id=user_id",
        table = "user_tags",
        column = "tag",
        indexed = true
)
private PersistentCollection<String> tags;
```

Many-to-many model collections name their join table explicitly:

```java
@ManyToMany(link = "id=id", joinTable = "user_friends")
private PersistentCollection<User> friends;
```

The generated insert builder accepts persistent values, including `@ForeignColumn` fields, but it does not generate setters for `Reference` or `PersistentCollection` fields. Set the linking column in the builder, call `Reference.set(...)` after insertion, mutate the collection, or use a `BatchInsert` when several related rows must be created together.

`@OneToOne(fkey = false)` disables creation of the PostgreSQL foreign-key constraint. `updateReferencedColumns = true` makes `Reference.set(...)` update the columns on the referenced side of the link instead of the holder side.

### Foreign-table values and insert strategies

`@ForeignColumn` lets one model expose a value physically stored in another table:

```java
@Insert(InsertStrategy.PREFER_EXISTING)
@ForeignColumn(
        name = "favorite_color",
        table = "user_preferences",
        link = "id=user_id",
        nullable = true
)
private PersistentValue<String> favoriteColor;
```

The generated `favoriteColor(...)` builder method inserts the linked foreign-table value along with the main model:

```java
User user = User.builder()
        .id(UUID.randomUUID())
        .name("Ada")
        .favoriteColor("blue")
        .insert(InsertMode.SYNC);
```

`PREFER_EXISTING` preserves a matching foreign row that already exists. `OVERWRITE_EXISTING` updates it with the builder's value.

### Deleting models and related data

Delete a model by calling `delete()`:

```java
user.delete();

if (user.isDeleted()) {
    // Persistent and cached wrappers on this object can no longer be used.
}

// Wait for the queued PostgreSQL work only when a blocking boundary is intended.
StaticData.flushTaskQueue();
```

`@Delete` controls the related rows represented by the annotated field:

| Strategy | General behavior |
| --- | --- |
| `CASCADE` | Deletes the related data. On a many-to-many collection, it removes matching join rows and deletes the referenced objects. |
| `SET_NULL` | Preserves the related data but removes the relationship. One-to-many linking columns are set to `null`; many-to-many join rows are removed. |
| `NO_ACTION` | Leaves the relation and related data unchanged. An enabled foreign-key constraint may reject the holder deletion. |

Choose `CASCADE` carefully for many-to-many relationships because referenced objects may also belong to other holders.

When `@Delete` is omitted, the strategy defaults to `NO_ACTION`. Fields that may be unlinked through `SET_NULL` need nullable linking columns.

There is currently no general `onDelete` callback for a `UniqueData` instance. Use collection `onRemove(...)`, reference `onUpdate(...)`, or application code surrounding `delete()` when those semantics are sufficient.

## Redis-backed values

`CachedValue` is keyed by its identifier plus the holder's ID columns:

```java
@Identifier("online")
@ExpireAfter(60)
private CachedValue<Boolean> online = CachedValue.of(this, Boolean.class)
        .withFallback(false)
        .onUpdate(User.class, (user, update) ->
                user.onOnlineStateChanged(update.oldValue(), update.newValue()));
```

`online.get()` reads the local mirrored value. `online.set(true)` updates that mirror and queues a Redis write. After 60 seconds Redis removes the key, and subsequent reads return the fallback. An expiration of `0` or less means no expiration.

For cached-value handlers, an absent or expired Redis value is presented as the configured fallback. Setting a cached value equal to its fallback removes the stored Redis value instead of storing a duplicate of the fallback.

A refresher calculates a missing or explicitly refreshed value:

```java
@Identifier("score")
@ExpireAfter(300)
private CachedValue<Integer> score = CachedValue.of(this, Integer.class)
        .withFallback(0)
        .refresher(User.class, (user, previousValue) ->
                user.calculateScore(previousValue));
```

- `score.get()` invokes the refresher only when the value is absent. A non-null result is stored and returned.
- `score.refresh()` invokes it explicitly with the current value, or the fallback when no Redis value exists, and then stores the result.
- `@UpdateInterval(milliseconds)` coalesces frequent Redis writes. Local values and local callbacks still change immediately.

## Additional usage patterns

### Batch inserts

`BatchInsert` groups generated builder inserts into one operation:

```java
import net.staticstudios.data.insert.BatchInsert;

import java.util.concurrent.CompletableFuture;

BatchInsert batch = StaticData.createBatchInsert();

CompletableFuture<User> adaFuture = User.builder()
        .id(UUID.randomUUID())
        .name("Ada")
        .insert(batch);

CompletableFuture<User> graceFuture = User.builder()
        .id(UUID.randomUUID())
        .name("Grace")
        .insert(batch);

batch.insert(InsertMode.SYNC);

User ada = adaFuture.join();
User grace = graceFuture.join();
```

The futures complete after the batch has inserted and its post-insert actions have run. A batch must contain at least one insert, can be executed only once, and supports only inserts—it is not a general transaction for arbitrary updates and deletes.

### Read-only snapshots

Use a snapshot when a callback, audit operation, or comparison needs a stable view:

```java
User before = StaticData.createSnapshot(user);
user.rename("New name");

System.out.println(before.name()); // value at snapshot creation
```

Snapshot wrappers reject mutation, and the snapshot is not marked deleted when the live object is deleted later. Snapshots are shallow for relationships: reference targets and collection membership are captured by ID, but accessing a related object resolves the live related instance.

### Update throttling

`@UpdateInterval` is useful for hot counters or frequently changing state:

```java
@Column(name = "views", nullable = true)
@UpdateInterval(5_000)
private PersistentValue<Integer> views;
```

Every `views.set(...)` updates H2 immediately. Only the latest source-store update for that holder within the five-second interval is queued, reducing pressure on the single PostgreSQL task queue.

### Runtime statistics

```java
StaticDataStatistics statistics = StaticData.getStatistics();

long readsPerSecond = statistics.getQueriesPerSecond();
long writesPerSecond = statistics.getUpdatesPerSecond();
long operationsPerSecond = statistics.getOperationsPerSecond();
int relationCacheEntries = statistics.getRelationCacheSize();
int cellCacheEntries = statistics.getCellCacheSize();
```

These statistics describe local H2 activity and internal cache sizes. They are useful for monitoring a running instance, not as PostgreSQL server metrics.

## Supported value types

Built-in persistent value types are:

- `String`
- `Integer`
- `Long`
- `Float`
- `Double`
- `Boolean`
- `UUID`
- `java.sql.Timestamp`
- Java enums, stored by name

Other types require a `ValueSerializer<D, S>`, where `D` is the application type and `S` is one of the built-in storage types:

```java
public record AccountSettings(String theme) {
}

public final class AccountSettingsSerializer
        implements ValueSerializer<AccountSettings, String> {

    @Override
    public AccountSettings deserialize(String serialized) {
        return new AccountSettings(serialized);
    }

    @Override
    public String serialize(AccountSettings settings) {
        return settings.theme();
    }

    @Override
    public Class<AccountSettings> getDeserializedType() {
        return AccountSettings.class;
    }

    @Override
    public Class<String> getSerializedType() {
        return String.class;
    }
}
```

Register serializers after `StaticData.init(...)` and before `StaticData.load(...)`:

```java
StaticData.registerValueSerializer(new AccountSettingsSerializer());
StaticData.load(Account.class);
StaticData.finishLoading();
```

Column nullability is controlled by `nullable` on `@Column` and `@ForeignColumn`. `@DefaultValue` is a database-level SQL default, not a Java-side default; its string must be valid for both PostgreSQL and H2.

## Consistency and write behavior

The in-memory database makes the common path fast, but it changes the consistency model:

- Reads and queries are synchronous local H2 operations.
- `InsertMode.ASYNC` commits to H2 first and queues the PostgreSQL insert. If PostgreSQL later rejects the insert, the H2 state is not rolled back.
- `InsertMode.SYNC` waits for the PostgreSQL insert attempt and is therefore blocking. If that background PostgreSQL task fails, the H2 insert is rolled back; the current implementation logs the task failure instead of propagating it to the caller.
- Value updates, collection changes, Redis writes, and deletes update the local state and propagate in the background.
- Other application instances see a PostgreSQL or Redis change after the notification reaches them. Cross-instance consistency is therefore eventual, not instantaneous.
- `@UpdateInterval` deliberately adds propagation delay and keeps only the latest queued value within the configured interval.

`StaticData.flushTaskQueue()` blocks until all work queued before the call has completed. It is useful in tests, before an orderly shutdown, or at a durability boundary; do not call it from a latency-sensitive thread.

## Schema behavior

Models are schema definitions as well as mappings. During `load(...)`, `static-data` creates missing schemas, tables, columns, indexes, foreign keys, triggers, and the PostgreSQL notification function.

Schema handling is additive. It does not rename or remove database objects, rewrite existing column types, or provide versioned migrations. Use a migration tool or reviewed SQL for those changes. Also review generated DDL expectations before pointing the ORM at a production database, because the configured PostgreSQL account requires DDL privileges.

Schema, table, column, identifier, and relationship names may contain environment substitutions such as `${APP_SCHEMA}`. Initialization fails if a referenced environment variable is not set.

## Current limitations

- **PostgreSQL and Redis are mandatory.** PostgreSQL is the only persistent SQL backend, and the current configuration always connects to Redis. There is no H2-free PostgreSQL-only mode or Redis-free mode.
- **Loaded tables must fit in memory.** `finishLoading()` copies every row of every registered table into H2. Startup time and heap usage grow with table size; row-level or partial-table loading and a disk-backed cache are not implemented.
- **Consistency is eventual for normal mutations.** Background PostgreSQL or Redis failures can leave the local cache ahead of the source store. A re-established PostgreSQL listener currently does not automatically perform a full H2 resynchronization, so changes missed during a connection interruption may remain stale until restart.
- **Synchronous insert failure reporting is incomplete.** `InsertMode.SYNC` waits and rolls back H2 when the queued PostgreSQL insert fails, but that queued failure is logged rather than rethrown to the caller.
- **Loading is a one-way lifecycle.** The global `StaticData` facade can be initialized once, and models cannot be added after `finishLoading()`.
- **Primary keys should be treated as immutable.** Updating an `@IdColumn` after insertion has known failing cases, especially when foreign keys and relationships depend on that ID.
- **Callback metadata is limited.** Handlers become active only after the model's first live instance is initialized. They do not expose the local/remote origin or a public insert/update/delete cause, and there is no general entity `onDelete` callback.
- **The compiler integration is Java 21/`javac` specific.** The generated builder/query API is injected by an annotation processor that uses `javac` internals. IntelliJ IDEA is the only IDE with dedicated integration in this repository; other compilers and IDEs are not currently supported.
- **Schema evolution is limited.** Automatic DDL creates missing objects but is not a general migration system. Changes such as renames, removals, type conversions, and modified existing foreign-key behavior require manual migrations.
- **Query generation has gaps.** Generated predicates cover persistent and cached values, but not `Reference` fields; query through their linking columns instead. Only one order-by field is retained. In `3.3.12-SNAPSHOT`, combining `orderBy...` with `limit(...)` or `offset(...)` emits clauses in an invalid order and should be avoided.
- **Many-to-many join customization is incomplete.** Join-table names can be configured, but join-column names cannot. This is especially restrictive for self-referential many-to-many relationships.
- **Snapshots are shallow across relationships.** A snapshot freezes its own values and relationship membership, but referenced entities resolve to live instances rather than recursively snapshotted objects.
- **Type support is deliberately small.** Custom values must serialize to a built-in type, and `byte[]` support is currently disabled.
- **Connection configuration is basic.** The public config exposes PostgreSQL host/port/database/username/password and Redis host/port. Redis authentication, Redis database selection, TLS, and advanced pool/driver options are not exposed.

## Repository modules

- `annotations`: public annotations and strategy enums
- `core`: runtime ORM, H2 cache, PostgreSQL synchronization, and Redis integration
- `processor`: Java annotation processor that creates builders and query builders
- `intellij-plugin`: IntelliJ IDEA awareness for the generated API
- `benchmark`: JMH microbenchmarks and container-backed Minecraft workloads; see [`benchmark/README.md`](benchmark/README.md)
- `utils`: shared internal utilities

The project is currently published as a snapshot. Expect API and behavior changes between snapshot versions.
