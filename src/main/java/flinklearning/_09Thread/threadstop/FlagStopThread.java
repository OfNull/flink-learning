package flinklearning._09Thread.threadstop;

import java.util.concurrent.TimeUnit;

public class FlagStopThread extends Thread {
    //中断标记
    private volatile boolean isRunning = true;

    @Override
    public void run() {
        for (int i = 1; i < 500001; i++) {
            if (isRunning) {
                System.out.println("ThreadName:" + this.getName() + " 计数：" + i);
            }else {
                return;
            }
        }
    }

    //停止
    public void stopRun() {
        this.isRunning = false;
    }

    public static void main(String[] args) {
        FlagStopThread flagStopThread = new FlagStopThread();
        flagStopThread.start();
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        flagStopThread.stopRun();
    }
}
