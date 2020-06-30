/*
 * Copyright (C) 2017 Julien Viet
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.vertx.sqlclient.impl.pool;

import io.vertx.core.*;
import io.vertx.core.impl.ContextInternal;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.impl.Connection;
import io.vertx.sqlclient.impl.command.CommandBase;
import io.vertx.sqlclient.spi.DatabaseMetadata;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * Alternative implementation of ConnectionPool inspired by Agroal's pool
 *
 * @author <a href="mailto:lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalConnectionPool {

  private final Consumer<Handler<AsyncResult<Connection>>> connector;
  private final ContextInternal context;
  private final int maxSize;
  private final int maxWaitQueueSize;
  private final ArrayDeque<Promise<Connection>> waiters = new ArrayDeque<>();
  private final List<PooledConnection> allConnections = new StampedCopyOnWriteArrayList<>(PooledConnection.class);
  private final LongAdder creation = new LongAdder();
  private ThreadLocal<List<PooledConnection>> localCache; // TODO: clear this from time to time

  private boolean checkInProgress;
  private boolean closed;

  public AgroalConnectionPool(Consumer<Handler<AsyncResult<Connection>>> connector, int maxSize) {
    this(connector, maxSize, PoolOptions.DEFAULT_MAX_WAIT_QUEUE_SIZE);
  }

  public AgroalConnectionPool(Consumer<Handler<AsyncResult<Connection>>> connector, int maxSize, int maxWaitQueueSize) {
    this(connector, null, maxSize, maxWaitQueueSize);
  }

  public AgroalConnectionPool(Consumer<Handler<AsyncResult<Connection>>> connector, Context context, int maxSize, int maxWaitQueueSize) {
    Objects.requireNonNull(connector, "No null connector");
    if (maxSize < 1) {
      throw new IllegalArgumentException("Pool max size must be > 0");
    }
    this.localCache = new PooledConnectionThreadLocal();
    this.maxSize = maxSize;
    this.context = (ContextInternal) context;
    this.maxWaitQueueSize = maxWaitQueueSize;
    this.connector = connector;
  }

  private static final class PooledConnectionThreadLocal extends ThreadLocal<List<PooledConnection>> {

    @Override
    protected List<PooledConnection> initialValue() {
      return new UncheckedArrayList<>( PooledConnection.class );
    }
  }

  public int available() {
    int available = 0;
    for (PooledConnection connection : allConnections) {
      if (connection.isAvailable()) {
        available++;
      }
    }
    return available;
  }

  public int size() {
    return allConnections.size();
  }

  public void acquire(Handler<AsyncResult<Connection>> waiter) {
    if (closed) {
      throw new IllegalStateException("Connection pool closed");
    }
    Promise<Connection> promise = Promise.promise();
    promise.future().setHandler(waiter);
    waiters.add(promise);
    check();
  }

  public void close() {
    if (closed) {
      throw new IllegalStateException("Connection pool already closed");
    }
    closed = true;
    for (PooledConnection pooled : allConnections) {
      pooled.close();
    }
    Future<Connection> failure = Future.failedFuture("Connection pool closed");
    for (Promise<Connection> pending : waiters) {
      try {
        pending.handle(failure);
      } catch (Exception ignore) {
      }
    }
  }

  private class PooledConnection implements Connection, Connection.Holder  {

    private final AtomicReferenceFieldUpdater<PooledConnection, State> stateUpdater = newUpdater( PooledConnection.class, State.class, "state" );

    private final Connection conn;
    private final AgroalConnectionPool pool;
    private Holder holder;
    private volatile State state;

    PooledConnection(Connection conn, AgroalConnectionPool pool) {
      this.conn = conn;
      this.pool = pool;
      this.state = State.IN_USE;
    }

    @Override
    public boolean isSsl() {
      return conn.isSsl();
    }

    public boolean isAvailable() {
      return stateUpdater.get(this).equals(State.AVAILABLE);
    }

    public boolean take() {
      return stateUpdater.compareAndSet(this, State.AVAILABLE, State.IN_USE);
    }

    @Override
    public DatabaseMetadata getDatabaseMetaData() {
      return conn.getDatabaseMetaData();
    }

    @Override
    public void schedule(CommandBase<?> cmd) {
      conn.schedule(cmd);
    }

    /**
     * Close the underlying connection
     */
    private void close() {
        conn.close(this);
    }

    @Override
    public void init(Holder holder) {
      if (this.holder != null) {
        throw new IllegalStateException();
      }
      this.holder = holder;
    }

    @Override
    public void close(Holder holder) {
      if (holder != this.holder) {
        String msg;
        if (this.holder == null) {
          msg = "Connection released twice";
        } else {
          msg = "Connection released by " + holder + " owned by " + this.holder;
        }
        // Log it ?
        throw new IllegalStateException(msg);
      }
      this.holder = null;
      stateUpdater.set(this, State.AVAILABLE);
      pool.localCache.get().add(this);
      pool.check();
    }

    @Override
    public void handleNotification(int processId, String channel, String payload) {
      if (holder != null) {
        holder.handleNotification(processId, channel, payload);
      }
    }

    @Override
    public void handleClosed() {
      if (pool.allConnections.remove(this)) {
        if (holder == null) {
          stateUpdater.set(this, State.AVAILABLE);
        } else {
          holder.handleClosed();
        }
        check();
      } else {
        throw new IllegalStateException();
      }
    }

    @Override
    public void handleException(Throwable err) {
      if (holder != null) {
        holder.handleException(err);
      }
    }

    @Override
    public int getProcessId() {
      return conn.getProcessId();
    }

    @Override
    public int getSecretKey() {
      return conn.getSecretKey();
    }

  }

  private enum State {
    AVAILABLE, IN_USE, UNAVAILABLE
  }

  private void check() {
    if (closed) {
      return;
    }
    if (!checkInProgress) {
      checkInProgress = true;
      try {
        Promise<Connection> waiter;
        while ((waiter = waiters.poll()) != null) {

          // thread-local cache
          List<PooledConnection> cached = localCache.get();
          while (!cached.isEmpty()) {
            PooledConnection proxy = cached.remove(cached.size() - 1);
            if (proxy.take()) {
              waiter.handle(Future.succeededFuture(proxy));
              return;
            }
          }

          // go through all connections
          for (PooledConnection proxy : allConnections) {
            if (proxy.take()) {
              waiter.handle(Future.succeededFuture(proxy));
              return;
            }
          }

          // no avail, attempt to get a slot to create new
          if (creation.intValue() + allConnections.size() < maxSize) {
            creation.increment();
            Handler<AsyncResult<Connection>> finalWaiter = waiter;
            connector.accept(ar -> {
              creation.decrement();
              if (ar.succeeded()) {
                Connection conn = ar.result();
                PooledConnection proxy = new PooledConnection(conn, this);
                allConnections.add(proxy);
                conn.init(proxy);
                finalWaiter.handle(Future.succeededFuture(proxy));
              } else {
                finalWaiter.handle(Future.failedFuture(ar.cause()));
                check();
              }
            });
          } else {
            // we wait
            waiters.add(waiter);

            // may need to reduce the queue if maxWaitQueueSize was exceeded
            if (maxWaitQueueSize >= 0) {
              while (waiters.size() > maxWaitQueueSize) {
                waiters.pollLast().handle(Future.failedFuture("Max waiter size reached"));
              }
            }
            break;
          }
        }
      } finally {
        checkInProgress = false;
      }
    }
  }
}
