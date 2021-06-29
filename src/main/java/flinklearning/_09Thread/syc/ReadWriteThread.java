package flinklearning._09Thread.syc;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReadWriteThread {
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private Map<String, Object> map = new HashMap<>(50);

    public Object read(String k) {
        Lock readLock = readWriteLock.readLock();
        try {
            return map.get(k);
        } finally {
            readLock.unlock();
        }
    }

    public void add(String k, Object v) {
        Lock writeLock = readWriteLock.writeLock();
        try {
            map.put(k, v);
        } finally {
            writeLock.unlock();
        }
    }

    public static void main(String[] args) {
        ReadWriteThread readWriteThread = new ReadWriteThread();
        for (int i = 0; i < 1; i++) {
            final String tmp = String.valueOf(i);
            new Thread(() -> readWriteThread.add(tmp, "---" + tmp)).start();
        }

//        for (int i = 0; i < 10; i++) {
//            final String tmp = String.valueOf(i);
//            new Thread(() -> readWriteThread.read(tmp)).start();
//        }
    }
}
