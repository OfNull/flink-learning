package flinklearning._09Thread;

public class ThreadStop {

    public static void main(String[] args) throws InterruptedException {
        MyThread myThread = new MyThread();
        myThread.start();
        Thread.sleep(1000L);
//        myThread.interrupt();
        Thread.currentThread().interrupt();
//        System.out.println(Thread.currentThread().getName() + "是否停止1 " + myThread.interrupted()); //测试的是当前线程 是否中断 myThread.interrupt() 中断的是 MyThread 所以返回false false
//        System.out.println(Thread.currentThread().getName() + "是否停止2 " + myThread.interrupted()); // 方法会有的状态 连续两次调用 interrupted  第二次返回 false 第一次
        System.out.println(Thread.currentThread().getName() + "是否停止1 " + myThread.isInterrupted()); // 方法会有的状态 连续两次调用 interrupted  第二次返回 false 第一次
        System.out.println(Thread.currentThread().getName() + "是否停止2 " + myThread.isInterrupted()); // 方法会有的状态 连续两次调用 interrupted  第二次返回 false 第一次
    }


    static class MyThread extends Thread {
        @Override
        public void run() {
            for (int i = 0; i < 500000; i++) {
//                System.out.println("print " + (i+1));
            }
        }
    }
}
