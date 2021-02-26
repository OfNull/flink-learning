package flinklearning.operator.entity;

public class Item {
    /**
     * 商品名
     */
    private String name;

    private String origin;
    /**
     * 商品数量
     */
    private int qty;

    public Item() {
    }

    public Item(String name, int qty) {
        this.name = name;
        this.qty = qty;
    }

    public Item(String name, String origin, int qty) {
        this.name = name;
        this.origin = origin;
        this.qty = qty;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getQty() {
        return qty;
    }

    public void setQty(int qty) {
        this.qty = qty;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    @Override
    public String toString() {
        return "Item{" +
                "name='" + name + '\'' +
                ", origin='" + origin + '\'' +
                ", qty=" + qty +
                '}';
    }
}
