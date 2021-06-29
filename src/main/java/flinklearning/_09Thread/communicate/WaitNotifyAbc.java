package flinklearning._09Thread.communicate;

public class WaitNotifyAbc {


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

        private String flg = "A";

        private synchronized void printA() throws InterruptedException {
            while (true) {
                while (!"A".equals(flg)) {
                    wait();
                }

                System.out.println("A");
                flg = "B";
                notifyAll();
            }
        }

        private synchronized void printB() throws InterruptedException {
            while (true) {
                while (!"B".equals(flg)) {
                    wait();
                }
                System.out.println("B");
                flg = "C";
                notifyAll();
            }
        }

        private synchronized void printC() throws InterruptedException {
            while (true) {
                while (!"C".equals(flg)) {
                    wait();
                }
                System.out.println("C");
                flg = "A";
                notifyAll();
            }
        }
    }
}
