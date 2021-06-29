package flinklearning._09Thread;

public class YieldDemo {

    public static void main(String[] args) {
        Print a = new Print("A");
        Print b = new Print("B");
        Print c = new Print("C");
        Thread t1 = new Thread() {
            @Override
            public void run() {

                a.print();
            }
        };

        Thread t2 = new Thread() {
            @Override
            public void run() {
                b.print();
            }
        };

        Thread t3 = new Thread() {
            @Override
            public void run() {
                Thread.yield();
                c.print();
            }
        };

        t1.start();
        t2.start();
        t3.start();

    }


    static class Print {
        private String msg;

        public Print(String msg) {
            this.msg = msg;
        }

        private void print() {
            System.out.println(msg);
        }
    }
}
