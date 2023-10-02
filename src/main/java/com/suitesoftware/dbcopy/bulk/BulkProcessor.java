package com.suitesoftware.dbcopy.bulk;

/**
 * User: lrb
 * Date: 9/24/16
 * Time: 10:39 AM
 * (c) Copyright Suite Business Software
 */

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class BulkProcessor implements AutoCloseable {

    private final ScheduledThreadPoolExecutor scheduler;
    private final ScheduledFuture<?> scheduledFuture;

    private volatile boolean closed = false;

    private final BulkWriteHandler handler;

    private final int bulkSize;

    private List<String []> batchedEntities;

    public BulkProcessor(BulkWriteHandler handler, int bulkSize) {
        this(handler, bulkSize, null);
    }

    public BulkProcessor(BulkWriteHandler handler, int bulkSize, Duration flushInterval) {

        this.handler = handler;
        this.bulkSize = bulkSize;

        // Start with an empty List of batched entities:
        this.batchedEntities = new ArrayList<>();

        if(flushInterval != null) {
            // Create a Scheduler for the time-based Flush Interval:
            this.scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);
            this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(new Flush(), flushInterval.toMillis(), flushInterval.toMillis(), TimeUnit.MILLISECONDS);
        } else {
            this.scheduler = null;
            this.scheduledFuture = null;
        }
    }

    public synchronized BulkProcessor add(String [] entity) {
        batchedEntities.add(entity);
        executeIfNeccessary();
        return this;
    }

    @Override
    public void close() throws Exception {
        // If the Processor has already been closed, do not proceed:
        if (closed) {
            return;
        }
        closed = true;

        // Quit the Scheduled FlushInterval Future:
        if (this.scheduledFuture != null) {
            cancel(this.scheduledFuture);
            this.scheduler.shutdown();
        }

        // Are there any entities left to write?
        if (batchedEntities.size() > 0) {
            execute();
        }
    }

    private void executeIfNeccessary() {
        if(batchedEntities.size() >= bulkSize) {
            execute();
        }
    }

    // (currently) needs to be executed under a lock
    private void execute() {
        // Assign to a new List:
        final List<String []> entities = batchedEntities;
        // We can restart batching entities:
        batchedEntities = new ArrayList<>();
        // Write the previously batched entities to PostgreSQL:
        write(entities);
    }

    private void write(List<String []> entities) {
        try {
            handler.write(entities);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean cancel(Future<?> future) {
        if (future != null) {
            return future.cancel(false);
        }
        return false;
    }

    class Flush implements Runnable {

        @Override
        public void run() {
            synchronized (BulkProcessor.this) {
                if (closed) {
                    return;
                }
                if (batchedEntities.size() == 0) {
                    return;
                }
                execute();
            }
        }
    }
}