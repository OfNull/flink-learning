package flinklearning._09Thread.syc;

public class SynThread extends Thread {
    private static int count = 500000;
    private Object lock;

    public SynThread() {

    }

    public SynThread(Object lock) {
        this.lock = lock;
    }

    @Override
    public void run() {
        synchronized (lock) {
            for (; count > 0; count--) {
                System.out.println(Thread.currentThread().getName() + " count: " + count);
            }
        }

    }

    public static void main(String[] args) {
        Object lock = new Object();
        for (int i = 5; i > 0; i--) {
            new SynThread(lock).start();
        }
    }

    public static synchronized void staticSyncMethod() {

    }

    public  synchronized void syncMethod() {

    }
}
