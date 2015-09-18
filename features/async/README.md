## Asynchronous programming

The driver exposes an asynchronous API that allows you to write programs
in a fully-non blocking manner. Asynchronous methods return instances of
Guava's [ListenableFuture], that can be conveniently chained and
composed.

Here is a short example that opens a session and runs a query
asynchronously:

```java
import com.google.common.util.concurrent.*;

ListenableFuture<Session> session = cluster.connectAsync();

// Use transform with an AsyncFunction to chain an async operation after another:
ListenableFuture<ResultSet> resultSet = Futures.transform(session,
    new AsyncFunction<Session, ResultSet>() {
        public ListenableFuture<ResultSet> apply(Session session) throws Exception {
            return session.executeAsync("select release_version from system.local");
        }
    });

// Use transform with a simple Function to apply a synchronous computation on the result:
ListenableFuture<String> version = Futures.transform(resultSet,
    new Function<ResultSet, String>() {
        public String apply(ResultSet rs) {
            return rs.one().getString("release_version");
        }
    });

// Use a callback to perform an action once the future is complete:
Futures.addCallback(version, new FutureCallback<String>() {
    public void onSuccess(String version) {
        System.out.printf("Cassandra version: %s%n", version);
    }

    public void onFailure(Throwable t) {
        System.out.printf("Failed to retrieve the version: %s%n",
            t.getMessage());
    }
});
```

### Good practices

If your callback is slow, consider providing a separate executor.
Otherwise the callback might run on one of the driver's I/O threads,
blocking I/O operations for other requests while it is running:

```java
ListenableFuture<String> result = Futures.transform(resultSet,
    new Function<ResultSet, String>() {
        public String apply(ResultSet rs) {
            return someVeryLongComputation(rs);
        }
    }, myCustomExecutor);
```

Avoid blocking operations in callbacks, especially if you don't provide
a separate executor. This could easily lead to deadlock if the thread
that's supposed to complete the blocking call is also the thread that's
waiting on it:

```java
ListenableFuture<ResultSet> resultSet = Futures.transform(session,
    new Function<Session, ResultSet>() {
        public ResultSet apply(Session session) {
            // Synchronous operation in a callback.
            // DON'T DO THIS! It might deadlock.
            return session.execute("select release_version from system.local");
        }
    });
```

### Known limitations

There are still a few places where the driver will block internally
(mainly for historical reasons):

* [Cluster#init][init] performs blocking I/O operations. To avoid
  issues, you should create your `Cluster` instances while bootstrapping
  your application, and call `init` immediately. If you need to create new
  instances at runtime, make sure this does not happen on an I/O thread.
* if a connection pool is busy, the driver will block until a connection
  becomes available. To avoid this, set
  [PoolingOptions.poolTimeoutMillis][setPoolTimeoutMillis] to 0; the
  driver will not block, just move to the next host immediately.
* if the session is set to a specific keyspace (either at startup or by
  issuing a `USE` statement on a running session), the keyspace needs to
  be propagated to any newly created connection. This is done when the
  connection is first borrowed from the connection pool, and currently
  blocks. To avoid any issue, only use a session with no keyspace set
  (i.e. created by `Cluster#connect()`). This will be addressed in
  [JAVA-893](https://datastax-oss.atlassian.net/browse/JAVA-893).
* trying to read fields from a [query trace] will block if the trace
  hasn't been fetched already.

[ListenableFuture]: https://code.google.com/p/guava-libraries/wiki/ListenableFutureExplained
[init]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/Cluster.html#init()
[setPoolTimeoutMillis]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/PoolingOptions.html#setPoolTimeoutMillis(int)
[query trace]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/QueryTrace.html
