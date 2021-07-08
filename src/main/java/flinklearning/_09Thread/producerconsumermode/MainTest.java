package flinklearning._09Thread.producerconsumermode;

import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MainTest {
    public static void main(String[] args) {
        Nver<String> nver = new Nver<>();

        for (int in = 0; in < 1; in++) {
            Thread producer = new Thread(() -> {
                while (true) {
                    int i = new Random().nextInt(1);
                    try {
                        nver.producer(String.format("%s - 当前时间：%s - 休眠：%s", Thread.currentThread().getName(), LocalDateTime.now(), i));
                        TimeUnit.MILLISECONDS.sleep(i);
                    } catch (InterruptedException e) {
                        System.out.println("生产者发生异常！ ");
                        e.printStackTrace();
                    }
                }

            });
            producer.setName("producer - " + in);
            producer.start();
        }

        for (int i = 0; i < 1; i++) {
            Thread consumer = new Thread(() -> {
                while (true) {
                    try {
                        String consumer1 = nver.consumer();
                        System.out.println(Thread.currentThread().getName() + "消费到数据：" + consumer1);
                    } catch (InterruptedException e) {
                        System.out.println("消费者发生异常！ ");
                        e.printStackTrace();
                    }
                }
            });

            consumer.start();
        }


    }
}
