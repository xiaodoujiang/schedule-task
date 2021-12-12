package cn.bmilk.scheduleTask;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ScheduleTask<V> extends FutureTask<V> implements RunnableScheduledFuture<V> {

    private static final AtomicLong sequencer = new AtomicLong();

    private final long sequenceNumber;

    private long time;

    private final AtomicInteger runTimes;

    private final long period;

    private final ThreadPoolExecutor threadPool;

    int heapIndex;

    /**
     * Creates a one-shot action with given nanoTime-based trigger time.
     */
    ScheduleTask(Runnable r, V result,  long delay, TimeUnit unit,long period, ThreadPoolExecutor threadPool) throws InterruptedException {
        super(r, result);
        this.time = triggerTime(delay ,unit);
        this.runTimes = new AtomicInteger(1);
        this.period = unit.toNanos(period);
        this.threadPool = threadPool;
        this.sequenceNumber = sequencer.getAndIncrement();
    }

    /**
     * Creates a periodic action with given nano time and period.
     */
    ScheduleTask(Runnable r, V result, long delay, TimeUnit unit, int runTimes, long period, ThreadPoolExecutor threadPool) throws InterruptedException {
        super(r, result);
        this.time = triggerTime(delay ,unit);
        this.runTimes = new AtomicInteger(runTimes);
        this.period = unit.toNanos(period);
        this.threadPool = threadPool;
        this.sequenceNumber = sequencer.getAndIncrement();
    }

    /**
     * Creates a one-shot action with given nanoTime-based trigger time.
     */
    ScheduleTask(Callable<V> callable, long delay, TimeUnit unit, long period, ThreadPoolExecutor threadPool) throws InterruptedException {
        super(callable);
        this.time = triggerTime(delay ,unit);
        this.runTimes = new AtomicInteger(1);
        this.period = unit.toNanos(period);
        this.threadPool = threadPool;
        this.sequenceNumber = sequencer.getAndIncrement();
    }

    /**
     * Creates a periodic action with given nano time and period.
     */
    ScheduleTask(Callable<V> callable, long delay, TimeUnit unit,  int runTimes, long period, ThreadPoolExecutor threadPool) throws InterruptedException {
        super(callable);
        this.time = triggerTime(delay ,unit);
        this.runTimes = new AtomicInteger(runTimes);
        this.period = unit.toNanos(period);
        this.threadPool = threadPool;
        this.sequenceNumber = sequencer.getAndIncrement();
    }


    @Override
    public void run() {
        System.out.println(this + "  run with " + runTimes + "  times"+"  +++ " + System.currentTimeMillis());
        if (!isPeriodic()) {
            super.run();
        } else if (super.runAndReset() && runTimes.getAndDecrement() > 1) {
            try {
                setNextRunTime();
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            }
            if (!queueAdd())
                reject(this);
        }
    }

    private long triggerTime(long delay, TimeUnit unit) throws InterruptedException {
        return triggerTime(unit.toNanos((delay < 0) ? 0 : delay));
    }


    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(time - now(), NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        if (other == this) // compare zero if same object
            return 0;
        if (other instanceof ScheduleTask) {
            ScheduleTask<?> x = (ScheduleTask<?>)other;
            long diff = time - x.time;
            if (diff < 0)
                return -1;
            else if (diff > 0)
                return 1;
            else if (sequenceNumber < x.sequenceNumber)
                return -1;
            else
                return 1;
        }
        long diff = getDelay(NANOSECONDS) - other.getDelay(NANOSECONDS);
        return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
    }

    @Override
    public boolean isPeriodic() {
        return 0 != period;
    }


    /**
     * Sets the next time to run for a periodic task.
     */
    private void setNextRunTime() throws InterruptedException {
        long p = period;
        if (p > 0)
            time += p;
        else
            time = triggerTime(-p);
    }


    /**
     * Returns the trigger time of a delayed action.
     */
    private long triggerTime(long delay) throws InterruptedException {
        return now() +
                ((delay < (Long.MAX_VALUE >> 1)) ? delay : overflowFree(delay));
    }


    /**
     * Returns current nanosecond time.
     */
    private final long now() {
        return System.nanoTime();
    }


    /**
     * Constrains the values of all delays in the queue to be within
     * Long.MAX_VALUE of each other, to avoid overflow in compareTo.
     * This may occur if a task is eligible to be dequeued, but has
     * not yet been, while some other task is added with a delay of
     * Long.MAX_VALUE.
     */
    private long overflowFree(long delay) throws InterruptedException {
        if (!(threadPool.getQueue() instanceof DelayQueue)) {
            throw new InterruptedException();
        }
        Delayed head = (Delayed) threadPool.getQueue().peek();
        if (head != null) {
            long headDelay = head.getDelay(NANOSECONDS);
            if (headDelay < 0 && (delay - headDelay < 0))
                delay = Long.MAX_VALUE + headDelay;
        }
        return delay;
    }

    private boolean queueAdd(){
        return threadPool.getQueue().add(this);
    }

    /**
     * Invokes the rejected execution handler for the given command.
     * Package-protected for use by ScheduledThreadPoolExecutor.
     */
    final void reject(Runnable command) {
        threadPool.getRejectedExecutionHandler().rejectedExecution(command, threadPool);
    }
}
