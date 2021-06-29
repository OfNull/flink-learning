package flinklearning._09Thread.pool;

import java.sql.Time;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.*;

public class ExecutorDemo {
    public static void main(String[] args) throws InterruptedException {
        ExecutorService pool1 = Executors.newCachedThreadPool();
        new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>());

        ExecutorService pool2 = Executors.newFixedThreadPool(10);
        new ThreadPoolExecutor(10, 10,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());

        ExecutorService pool3 = Executors.newSingleThreadExecutor();
        new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());


        Executor pool4 = new ThreadPoolExecutor(10, 20, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), Executors.defaultThreadFactory(), new ThreadPoolExecutor.DiscardPolicy());

//

       Semaphore semaphore = new Semaphore(5);

        System.out.println("--------------------");
        for (int i = 0; i < 10; i++) {
            final int t = i;
            pool4.execute(() -> {
                try {
                    try {
                        semaphore.acquire(1); //获取令牌
                        TimeUnit.SECONDS.sleep(2);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName() + "---" + t + " -1 我执行完了 "+ LocalDateTime.now());
                } finally {
                    semaphore.release();
                }
            });
        }

        System.out.println("开始输出");
        ((ThreadPoolExecutor) pool4).shutdown();

    }
}
