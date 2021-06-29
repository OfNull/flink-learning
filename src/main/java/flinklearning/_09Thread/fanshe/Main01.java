package flinklearning._09Thread.fanshe;

import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;

public class Main01 {
    public static void main(String[] args) {
        String classPath = "flinklearning._09Thread.fanshe.Person";
        try {
            Class<?> aClass = Class.forName(classPath);
            aClass.getName();
            System.out.println("----------");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
