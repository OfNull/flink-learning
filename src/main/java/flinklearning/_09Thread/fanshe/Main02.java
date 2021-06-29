package flinklearning._09Thread.fanshe;

import java.lang.reflect.*;

public class Main02 {
    public static void main(String[] args) {
        Person person = new Person();
        Class personClass = person.getClass();

        Constructor[] declaredConstructors = personClass.getDeclaredConstructors(); //获取构造函数列表
        for (Constructor declaredConstructor : declaredConstructors) {
            System.out.println("第一个构造方法：");
            int modifiers = declaredConstructor.getModifiers();//修饰符getModifiers
            Parameter[] parameters = declaredConstructor.getParameters(); //获取参数 
            for (Parameter parameter : parameters) {
                int modifiers1 = parameter.getModifiers();//参数修饰符
                Class<?> type = parameter.getType();//参数类型
                String name = parameter.getName(); //参数名
                System.out.println(String.format("构造器修饰符：%s - 参数修饰符 %s - 参数类型 %s - 参数名称 %s ", Modifier.toString(modifiers), Modifier.toString(modifiers1), type, name));
            }
        }

        //获取指定参数的构造方法
        Class[] p = {String.class};
        try {
            Constructor declaredConstructor = personClass.getDeclaredConstructor(p);
            declaredConstructor.setAccessible(true); //private修饰的私有方法  设置为访问权限开放
            Person zhoukun = (Person) declaredConstructor.newInstance("zhoukun");
            System.out.println(zhoukun.getName());

            Class[] p2 = {Integer.TYPE};
            Method echo = personClass.getDeclaredMethod("echo", p2);
            echo.setAccessible(true);
            Object invoke = echo.invoke(zhoukun, 20);
            System.out.println("调用私有方法");

            //私有属性
            Field msg = personClass.getDeclaredField("msg");
            msg.setAccessible(true);
            msg.set(zhoukun, "假设");

            System.out.println("打印完毕字段结果： " + msg.get(zhoukun).toString());

        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }
}
