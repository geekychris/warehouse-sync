package com.warehouse.sync.scheduler;

import com.warehouse.sync.config.SyncConfig;
import com.warehouse.sync.engine.SyncEngine;
import com.warehouse.sync.engine.SyncResult;
import it.sauronsoftware.cron4j.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Long-running scheduler for the `serve` subcommand.
 *
 * cron4j only accepts minute-granularity cron patterns, so shorter cadences
 * (30s, 15s) are honored by an internal ScheduledExecutorService instead.
 *
 * Concurrency: at most one run per sync_id at a time — a second trigger
 * while a run is in flight is skipped and logged. If a sync must catch up,
 * the next tick will find fresh rows and drain them.
 *
 * Shutdown: install a SIGTERM/SIGINT hook that stops accepting new runs,
 * waits for in-flight ones to finish, then releases the main-thread latch.
 */
public class DaemonScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(DaemonScheduler.class);

    private final SyncEngine engine;
    private final List<SyncConfig> configs;
    private final Scheduler cron = new Scheduler();
    private final ScheduledExecutorService subMinute = Executors.newScheduledThreadPool(4, r -> {
        Thread t = new Thread(r, "sync-sub-minute");
        t.setDaemon(true);
        return t;
    });
    private final Map<String, ReentrantLock> runLocks = new ConcurrentHashMap<>();
    private final Map<String, Instant> lastFullRefresh = new HashMap<>();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    public DaemonScheduler(SyncEngine engine, List<SyncConfig> configs) {
        this.engine = engine;
        this.configs = configs;
    }

    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::gracefulStop, "sync-shutdown"));
        for (SyncConfig cfg : configs) {
            schedule(cfg);
        }
        cron.start();
        LOG.info("scheduler up — {} sync(s) scheduled", configs.size());
    }

    public void awaitShutdown() throws InterruptedException {
        stopLatch.await();
    }

    private void schedule(SyncConfig cfg) {
        runLocks.putIfAbsent(cfg.getSyncId(), new ReentrantLock());
        Duration cadence = cfg.getSchedule().cadenceDuration();

        Runnable task = () -> triggerRun(cfg);
        if (cadence.getSeconds() < 60) {
            long everyMillis = cadence.toMillis();
            subMinute.scheduleAtFixedRate(task, everyMillis, everyMillis, TimeUnit.MILLISECONDS);
            LOG.info("[{}] scheduled every {}ms (sub-minute)", cfg.getSyncId(), everyMillis);
        } else {
            String cronExpr = toCron(cadence);
            this.cron.schedule(cronExpr, task);
            LOG.info("[{}] scheduled cron='{}' (from cadence={})", cfg.getSyncId(), cronExpr, cadence);
        }
    }

    private void triggerRun(SyncConfig cfg) {
        if (shutdown.get()) return;
        ReentrantLock lock = runLocks.get(cfg.getSyncId());
        if (!lock.tryLock()) {
            LOG.warn("[{}] previous run still in flight — skipping this tick", cfg.getSyncId());
            return;
        }
        try {
            boolean dueForFull = isDueForFullRefresh(cfg);
            SyncResult r = dueForFull ? engine.runFullRefresh(cfg) : engine.run(cfg);
            if (dueForFull) lastFullRefresh.put(cfg.getSyncId(), Instant.now());
            LOG.info("[{}] {}", cfg.getSyncId(), r);
        } catch (Throwable t) {
            LOG.error("[{}] unhandled error during sync", cfg.getSyncId(), t);
        } finally {
            lock.unlock();
        }
    }

    private boolean isDueForFullRefresh(SyncConfig cfg) {
        Duration cadence = cfg.getSchedule().fullRefreshCadenceDuration();
        if (cadence == null) return false;
        Instant last = lastFullRefresh.get(cfg.getSyncId());
        if (last == null) return true;    // first tick after startup
        return Duration.between(last, Instant.now()).compareTo(cadence) >= 0;
    }

    private void gracefulStop() {
        if (!shutdown.compareAndSet(false, true)) return;
        LOG.info("shutdown: draining scheduler...");
        cron.stop();
        subMinute.shutdown();
        try { subMinute.awaitTermination(60, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        // Wait for any in-flight run to finish by attempting to acquire each lock.
        for (Map.Entry<String, ReentrantLock> e : runLocks.entrySet()) {
            try {
                if (e.getValue().tryLock(30, TimeUnit.SECONDS)) {
                    e.getValue().unlock();
                } else {
                    LOG.warn("shutdown: [{}] still holding lock after 30s — abandoning", e.getKey());
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        LOG.info("shutdown: complete");
        stopLatch.countDown();
    }

    /** Convert a Duration to a cron4j 5-field cron string (minute granularity). */
    static String toCron(Duration d) {
        long minutes = Math.max(1, d.toMinutes());
        if (minutes < 60) return "*/" + minutes + " * * * *";
        long hours = d.toHours();
        if (hours < 24) return "0 */" + hours + " * * *";
        return "0 0 */" + d.toDays() + " * *";
    }
}
