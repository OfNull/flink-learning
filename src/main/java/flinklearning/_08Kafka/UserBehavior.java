package flinklearning._08Kafka;

import java.io.Serializable;

public class UserBehavior implements Serializable {
    // 用户id
    private Long userId;
    // 商品id
    private Long itemId;
    // 品类id
    private Long catId;
    // 用户行为
    private String action;
    //所在省
    private Integer province;
    //事件时间
    private Long ts;

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Long getCatId() {
        return catId;
    }

    public void setCatId(Long catId) {
        this.catId = catId;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Integer getProvince() {
        return province;
    }

    public void setProvince(Integer province) {
        this.province = province;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }
}
