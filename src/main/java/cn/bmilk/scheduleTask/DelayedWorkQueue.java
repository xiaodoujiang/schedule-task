package cn.bmilk.scheduleTask;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class DelayedWorkQueue extends AbstractQueue<Runnable>
        implements BlockingQueue<Runnable> {

    /*
     * A DelayedWorkQueue is based on a heap-based data structure
     * like those in DelayQueue and PriorityQueue, except that
     * every ScheduledFutureTask also records its index into the
     * heap array. This eliminates the need to find a task upon
     * cancellation, greatly speeding up removal (down from O(n)
     * to O(log n)), and reducing garbage retention that would
     * otherwise occur by waiting for the element to rise to top
     * before clearing. But because the queue may also hold
     * RunnableScheduledFutures that are not ScheduledFutureTasks,
     * we are not guaranteed to have such indices available, in
     * which case we fall back to linear search. (We expect that
     * most tasks will not be decorated, and that the faster cases
     * will be much more common.)
     *
     * All heap operations must record index changes -- mainly
     * within siftUp and siftDown. Upon removal, a task's
     * heapIndex is set to -1. Note that ScheduledFutureTasks can
     * appear at most once in the queue (this need not be true for
     * other kinds of tasks or work queues), so are uniquely
     * identified by heapIndex.
     */

    private static final int INITIAL_CAPACITY = 16;
    private RunnableScheduledFuture<?>[] queue =
            new RunnableScheduledFuture<?>[INITIAL_CAPACITY];
    private final ReentrantLock lock = new ReentrantLock();
    /**
     * 阻塞队列大小
     * >=0 有界队列
     * <0 无界队列
     */
    private final int capacity;
    private int size = 0;

    /**
     * Thread designated to wait for the task at the head of the
     * queue.  This variant of the Leader-Follower pattern
     * (http://www.cs.wustl.edu/~schmidt/POSA/POSA2/) serves to
     * minimize unnecessary timed waiting.  When a thread becomes
     * the leader, it waits only for the next delay to elapse, but
     * other threads await indefinitely.  The leader thread must
     * signal some other thread before returning from take() or
     * poll(...), unless some other thread becomes leader in the
     * interim.  Whenever the head of the queue is replaced with a
     * task with an earlier expiration time, the leader field is
     * invalidated by being reset to null, and some waiting
     * thread, but not necessarily the current leader, is
     * signalled.  So waiting threads must be prepared to acquire
     * and lose leadership while waiting.
     */
    private Thread leader = null;

    /**
     * Condition signalled when a newer task becomes available at the
     * head of the queue or a new thread may need to become leader.
     */
    private final Condition available = lock.newCondition();

    /**
     * Creates an {@code DelayedWorkQueue} with the given (fixed)
     * capacity, the specified access policy and initially containing the
     * elements of the given collection,
     * added in traversal order of the collection's iterator.
     *
     * @param capacity the capacity of this queue,if {}
     */
    public DelayedWorkQueue(int capacity) {
        this.capacity = capacity;
        if (capacity < INITIAL_CAPACITY)
            queue = new RunnableScheduledFuture<?>[capacity];
    }

    public DelayedWorkQueue() {
        this.capacity = -1;
    }

    /**
     * Sets f's heapIndex if it is a ScheduledFutureTask.
     */
    private void setIndex(RunnableScheduledFuture<?> f, int idx) {
        if (f instanceof ScheduleTask)
            ((ScheduleTask<?>)f).heapIndex = idx;
    }

    /**
     * Sifts element added at bottom up to its heap-ordered spot.
     * Call only when holding lock.
     */
    private void siftUp(int k, RunnableScheduledFuture<?> key) {
        while (k > 0) {
            int parent = (k - 1) >>> 1;
            RunnableScheduledFuture<?> e = queue[parent];
            if (key.compareTo(e) >= 0)
                break;
            queue[k] = e;
            setIndex(e, k);
            k = parent;
        }
        queue[k] = key;
        setIndex(key, k);
    }

    /**
     * Sifts element added at top down to its heap-ordered spot.
     * Call only when holding lock.
     */
    private void siftDown(int k, RunnableScheduledFuture<?> key) {
        int half = size >>> 1;
        while (k < half) {
            int child = (k << 1) + 1;
            RunnableScheduledFuture<?> c = queue[child];
            int right = child + 1;
            if (right < size && c.compareTo(queue[right]) > 0)
                c = queue[child = right];
            if (key.compareTo(c) <= 0)
                break;
            queue[k] = c;
            setIndex(c, k);
            k = child;
        }
        queue[k] = key;
        setIndex(key, k);
    }

    /**
     * Resizes the heap array.  Call only when holding lock.
     */
    private void grow() {
        int oldCapacity = queue.length;
        int newCapacity = oldCapacity + (oldCapacity >> 1); // grow 50%
        if (capacity >= 0 && newCapacity > capacity) // 有界队列为了不浪费内存 限制最大容量
            newCapacity = capacity;
        if (newCapacity < 0) // overflow
            newCapacity = Integer.MAX_VALUE;
        queue = Arrays.copyOf(queue, newCapacity);
    }

    /**
     * Finds index of given object, or -1 if absent.
     */
    private int indexOf(Object x) {
        if (x != null) {
            if (x instanceof ScheduleTask) {
                int i = ((ScheduleTask) x).heapIndex;
                // Sanity check; x could conceivably be a
                // ScheduledFutureTask from some other pool.
                if (i >= 0 && i < size && queue[i] == x)
                    return i;
            } else {
                for (int i = 0; i < size; i++)
                    if (x.equals(queue[i]))
                        return i;
            }
        }
        return -1;
    }

    public boolean contains(Object x) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return indexOf(x) != -1;
        } finally {
            lock.unlock();
        }
    }

    public boolean remove(Object x) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            int i = indexOf(x);
            if (i < 0)
                return false;

            setIndex(queue[i], -1);
            int s = --size;
            RunnableScheduledFuture<?> replacement = queue[s];
            queue[s] = null;
            if (s != i) {
                siftDown(i, replacement);
                if (queue[i] == replacement)
                    siftUp(i, replacement);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return size;
        } finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public int remainingCapacity() {

        if(capacity<0)
            return Integer.MAX_VALUE;
        else {
            lock.lock();
            try {
                return capacity - size;
            }finally {
                lock.unlock();
            }
        }
    }

    public RunnableScheduledFuture<?> peek() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return queue[0];
        } finally {
            lock.unlock();
        }
    }

    public boolean offer(Runnable x) {
        if (x == null)
            throw new NullPointerException();
        RunnableScheduledFuture<?> e = (RunnableScheduledFuture<?>)x;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            int i = size;
            if (i >= capacity) {
                return false;
            }
            if (i >= queue.length)
                grow();
            size = i + 1;
            if (i == 0) {
                queue[0] = e;
                setIndex(e, 0);
            } else {
                siftUp(i, e);
            }
            if (queue[0] == e) {
                leader = null;
                available.signal();
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

    public void put(Runnable e) {
        offer(e);
    }

    public boolean add(Runnable e) {
        return offer(e);
    }

    public boolean offer(Runnable e, long timeout, TimeUnit unit) {
        return offer(e);
    }

    /**
     * Performs common bookkeeping for poll and take: Replaces
     * first element with last and sifts it down.  Call only when
     * holding lock.
     * @param f the task to remove and return
     */
    private RunnableScheduledFuture<?> finishPoll(RunnableScheduledFuture<?> f) {
        int s = --size;
        RunnableScheduledFuture<?> x = queue[s];
        queue[s] = null;
        if (s != 0)
            siftDown(0, x);
        setIndex(f, -1);
        return f;
    }

    public RunnableScheduledFuture<?> poll() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            RunnableScheduledFuture<?> first = queue[0];
            if (first == null || first.getDelay(NANOSECONDS) > 0)
                return null;
            else
                return finishPoll(first);
        } finally {
            lock.unlock();
        }
    }

    public RunnableScheduledFuture<?> take() throws InterruptedException {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (;;) {
                RunnableScheduledFuture<?> first = queue[0];
                if (first == null)
                    available.await();
                else {
                    long delay = first.getDelay(NANOSECONDS);
                    if (delay <= 0)
                        return finishPoll(first);
                    first = null; // don't retain ref while waiting
                    if (leader != null)
                        available.await();
                    else {
                        Thread thisThread = Thread.currentThread();
                        leader = thisThread;
                        try {
                            available.awaitNanos(delay);
                        } finally {
                            if (leader == thisThread)
                                leader = null;
                        }
                    }
                }
            }
        } finally {
            if (leader == null && queue[0] != null)
                available.signal();
            lock.unlock();
        }
    }

    public RunnableScheduledFuture<?> poll(long timeout, TimeUnit unit)
            throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (;;) {
                RunnableScheduledFuture<?> first = queue[0];
                if (first == null) {
                    if (nanos <= 0)
                        return null;
                    else
                        nanos = available.awaitNanos(nanos);
                } else {
                    long delay = first.getDelay(NANOSECONDS);
                    if (delay <= 0)
                        return finishPoll(first);
                    if (nanos <= 0)
                        return null;
                    first = null; // don't retain ref while waiting
                    if (nanos < delay || leader != null)
                        nanos = available.awaitNanos(nanos);
                    else {
                        Thread thisThread = Thread.currentThread();
                        leader = thisThread;
                        try {
                            long timeLeft = available.awaitNanos(delay);
                            nanos -= delay - timeLeft;
                        } finally {
                            if (leader == thisThread)
                                leader = null;
                        }
                    }
                }
            }
        } finally {
            if (leader == null && queue[0] != null)
                available.signal();
            lock.unlock();
        }
    }

    public void clear() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            for (int i = 0; i < size; i++) {
                RunnableScheduledFuture<?> t = queue[i];
                if (t != null) {
                    queue[i] = null;
                    setIndex(t, -1);
                }
            }
            size = 0;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns first element only if it is expired.
     * Used only by drainTo.  Call only when holding lock.
     */
    private RunnableScheduledFuture<?> peekExpired() {
        // assert lock.isHeldByCurrentThread();
        RunnableScheduledFuture<?> first = queue[0];
        return (first == null || first.getDelay(NANOSECONDS) > 0) ?
                null : first;
    }

    public int drainTo(Collection<? super Runnable> c) {
        if (c == null)
            throw new NullPointerException();
        if (c == this)
            throw new IllegalArgumentException();
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            RunnableScheduledFuture<?> first;
            int n = 0;
            while ((first = peekExpired()) != null) {
                c.add(first);   // In this order, in case add() throws.
                finishPoll(first);
                ++n;
            }
            return n;
        } finally {
            lock.unlock();
        }
    }

    public int drainTo(Collection<? super Runnable> c, int maxElements) {
        if (c == null)
            throw new NullPointerException();
        if (c == this)
            throw new IllegalArgumentException();
        if (maxElements <= 0)
            return 0;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            RunnableScheduledFuture<?> first;
            int n = 0;
            while (n < maxElements && (first = peekExpired()) != null) {
                c.add(first);   // In this order, in case add() throws.
                finishPoll(first);
                ++n;
            }
            return n;
        } finally {
            lock.unlock();
        }
    }

    public Object[] toArray() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return Arrays.copyOf(queue, size, Object[].class);
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (a.length < size)
                return (T[]) Arrays.copyOf(queue, size, a.getClass());
            System.arraycopy(queue, 0, a, 0, size);
            if (a.length > size)
                a[size] = null;
            return a;
        } finally {
            lock.unlock();
        }
    }

    public Iterator<Runnable> iterator() {
        return new Itr(Arrays.copyOf(queue, size));
    }

    /**
     * Snapshot iterator that works off copy of underlying q array.
     */
    private class Itr implements Iterator<Runnable> {
        final RunnableScheduledFuture<?>[] array;
        int cursor = 0;     // index of next element to return
        int lastRet = -1;   // index of last element, or -1 if no such

        Itr(RunnableScheduledFuture<?>[] array) {
            this.array = array;
        }

        public boolean hasNext() {
            return cursor < array.length;
        }

        public Runnable next() {
            if (cursor >= array.length)
                throw new NoSuchElementException();
            lastRet = cursor;
            return array[cursor++];
        }

        public void remove() {
            if (lastRet < 0)
                throw new IllegalStateException();
            DelayedWorkQueue.this.remove(array[lastRet]);
            lastRet = -1;
        }
    }
}
