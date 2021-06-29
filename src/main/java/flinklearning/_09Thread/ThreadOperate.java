package flinklearning._09Thread;

import java.util.concurrent.TimeUnit;

public class ThreadOperate extends Thread {
    @Override
    public void run() {
        System.out.println("线程名称:" + Thread.currentThread().getName() + " 线程唯一Id: " + this.getId() + " t1是否活着：" + this.isAlive());
        try {
            Thread.sleep(1000L); //休眠1秒
            TimeUnit.SECONDS.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ThreadOperate t1 = new ThreadOperate();
        System.out.println("01 t1是否活着: " + t1.isAlive());
        t1.start();
        try {
            Thread.sleep(500L); //休眠1秒
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("02 t1是否活着: " + t1.isAlive());
        try {
            Thread.sleep(1000L); //休眠1秒
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("03 t1是否活着: " + t1.isAlive());
    }
}
