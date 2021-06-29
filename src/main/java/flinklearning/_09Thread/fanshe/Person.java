package flinklearning._09Thread.fanshe;

public class Person {
    private String name;

    private int age;

    private String msg;

    public Person() {
    }

    public Person(String name, int age, String msg) {
        this.name = name;
        this.age = age;
        this.msg = msg;
    }

    private Person(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
    //私有方法
    private void echo(int age) {
        System.out.println("我叫：" + name + " 新的年龄 " + age);
    }
}
