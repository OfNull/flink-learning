package flinklearning._09Thread.syc;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockThread extends Thread {
    private static int count = 500000;
    static Lock lock = new ReentrantLock(); //创建可重入锁



    @Override
    public void run() {

        lock.lock();  //加锁
        try {
            for (; count > 0; count--) {
                System.out.println(Thread.currentThread().getName() + " count: " + count);
            }
        } finally {
            lock.unlock(); //解锁
        }
    }

    public static void main(String[] args) {
        for (int i = 5; i > 0; i--) {
            new LockThread().start();
        }
    }
}
