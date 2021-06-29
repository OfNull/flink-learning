package flinklearning._09Thread.communicate;

import java.util.concurrent.TimeUnit;

public class WaitNotify {

    public static void main(String[] args) throws InterruptedException {
        Operate operate = new Operate();
        Thread ta = new Thread(() -> operate.printA());
        Thread tb = new Thread(() -> operate.printB());
        ta.start();
        TimeUnit.MILLISECONDS.sleep(100);
        tb.start();
    }


    static class Operate {

        private volatile boolean isA = true;

        private synchronized void printA() {
            while (true) {
                while (!isA) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("A");
                isA = false;
                notifyAll();
            }
        }

        private synchronized void printB() {
            while (true) {
                while (isA) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("B");
                isA = true;
                notifyAll();
            }
        }
    }
}
