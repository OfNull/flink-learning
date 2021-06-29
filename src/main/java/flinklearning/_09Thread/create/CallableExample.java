package flinklearning._09Thread.create;

import java.util.concurrent.*;

public class CallableExample implements Callable<String> {
    @Override
    public String call() throws Exception {
        System.out.println("step 01 .... ");
        TimeUnit.SECONDS.sleep(6);
        System.out.println("step 02 .... ");
        int i = 5 / 0;
        return Thread.currentThread().getName() + " This is Callable";
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        CallableExample callableExample = new CallableExample();
        FutureTask<String> futureTask = new FutureTask<>(callableExample);
        Thread thread = new Thread(futureTask);
        thread.start();

        System.out.println("main .... ");
//        String s = futureTask.get();
        try {
            String s1 = futureTask.get(7, TimeUnit.SECONDS);
            System.out.println("----------" + s1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
