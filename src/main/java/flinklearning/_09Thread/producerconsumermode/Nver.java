package flinklearning._09Thread.producerconsumermode;

/**
 * 基于线程的生产消费模型
 */
public class Nver<IN> {

    private Object lock = new Object();
    /**
     * 元素
     */
    private IN element;
    /**
     * 是否被唤醒
     */
    private boolean wakeProducer;

    public void initWakeProducer() {
        wakeProducer = true;
    }


    public void producer(IN in) throws InterruptedException {

        synchronized (lock) {
            if (element != null && !wakeProducer) {
                lock.wait();
            }

            wakeProducer = false;

            //一定被别人唤醒
            if (element != null) {
                throw new RuntimeException(Thread.currentThread().getName()+" 唤醒异常！");
            }
            System.out.println(Thread.currentThread().getName()+"发送数据成功！");
            element = in;
            lock.notifyAll();
        }
    }


    public IN consumer() throws InterruptedException {
        IN in = null;
        synchronized (lock) {
            if (element == null) {
                lock.wait();
            }

            if (element != null) {
                in = element;
                element = null;
                lock.notifyAll();
            }

            return in;
        }
    }

}
