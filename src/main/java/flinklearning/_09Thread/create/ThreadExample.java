package flinklearning._09Thread.create;

public class ThreadExample extends Thread {
    @Override
    public void run() {
        System.out.println("ThreadName: " + Thread.currentThread().getName());
    }

    public static void main(String[] args) {
        ThreadExample threadExample = new ThreadExample();
        threadExample.start();
    }
}
