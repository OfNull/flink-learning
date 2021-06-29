package flinklearning._09Thread.threadstop;

import java.util.concurrent.TimeUnit;

public class InterruptStopThread extends Thread {

    @Override
    public void run() {
        try {
            for (int i = 1; i < 500001; i++) {
                if (!this.isInterrupted()) { //是否中断
                    System.out.println("ThreadName:" + this.getName() + " 计数：" + i);
                } else {
                    throw new InterruptedException();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("结束！");
        }
    }

//    public static void main(String[] args) {
//        InterruptStopThread stopThread = new InterruptStopThread();
//        stopThread.start();
//        try {
//            TimeUnit.SECONDS.sleep(1);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        stopThread.interrupt(); //标记中断
//        System.out.println("ThreadName:"+Thread.currentThread().getName()+" _ " + stopThread.interrupted()); //主线并没有中断，所以返回flase
//    }

    public static void main(String[] args) {
        Thread.currentThread().interrupt();
        System.out.println("ThreadName:"+Thread.currentThread().getName()+" _ " + Thread.interrupted()); //主线中断，所以返回true
        Thread.currentThread().interrupt();
        System.out.println("ThreadName:"+Thread.currentThread().getName()+" _ " + Thread.interrupted()); //第二次重置了状态 所以返回flase
    }
}
