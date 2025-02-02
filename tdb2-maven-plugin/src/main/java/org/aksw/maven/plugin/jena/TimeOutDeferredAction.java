package org.aksw.maven.plugin.jena;

import java.util.Objects;

/** Run an action at a certain interval - triggered by calls to tick(). */
public class TimeOutDeferredAction {
    private long lastTickInMillis;
    private long intervalInMillis;
    private Runnable action;

    private TimeOutDeferredAction(long intervalInMillis, Runnable action) {
        super();
        if (intervalInMillis < 0) {
            throw new IllegalArgumentException("Negative interval: " + intervalInMillis);
        }
        this.intervalInMillis = intervalInMillis;
        this.action = Objects.requireNonNull(action);
        this.lastTickInMillis = System.currentTimeMillis();
    }

    public static TimeOutDeferredAction of(long intervalInMillis, Runnable action) {
        return new TimeOutDeferredAction(intervalInMillis, action);
    }

    public void tick() {
        long time = System.currentTimeMillis();
        long delta = time - lastTickInMillis;
        if (delta >= intervalInMillis) {
            action.run();
        }
        lastTickInMillis = time;
    }

    public void forceTick() {
        action.run();
        lastTickInMillis = System.currentTimeMillis();
    }
}
