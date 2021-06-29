package flinklearning._09Thread.communicate;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConditionAbc {

    public static void main(String[] args) {
        Operate operate = new Operate();
        Thread ta = new Thread(()-> {
            try {
                operate.printA();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        Thread tb = new Thread(()-> {
            try {
                operate.printB();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        Thread tc = new Thread(()-> {
            try {
                operate.printC();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        ta.start();
        tb.start();
        tc.start();
    }

    static class Operate {
        Lock lock = new ReentrantLock();
        Condition conditionA = lock.newCondition();
        Condition conditionB = lock.newCondition();
        Condition conditionC = lock.newCondition();

        private String flg = "A";

        private void printA() throws InterruptedException {
            lock.lock();
            try {
                while (true) {
                    while (!"A".equals(flg)) {
                        conditionA.await();
                    }
                    System.out.println("A");
                    flg = "B";
                    conditionB.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }

        private void printB() throws InterruptedException {
            lock.lock();
            try {
                while (true) {
                    while (!"B".equals(flg)) {
                        conditionB.await();
                    }
                    System.out.println("B");
                    flg = "C";
                    conditionC.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }

        private void printC() throws InterruptedException {
            lock.lock();
            try {
                while (true) {
                    while (!"C".equals(flg)) {
                        conditionC.await();
                    }

                    System.out.println("C");
                    flg = "A";
                    conditionA.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
