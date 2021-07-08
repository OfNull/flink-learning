package flinklearning._06cdc;


import java.util.Map;

/**
 * Binlog Json对象
 */
public class BinlogInfo {
    private String db;
    private String table;
    private Map<String, Object> after;   //更新后值
    private Map<String, Object> before;  //更新之前值
    private String op;                   //操作类型
    private long ts_ms;                  //事件时间戳

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Map<String, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }

    public Map<String, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public long getTs_ms() {
        return ts_ms;
    }

    public void setTs_ms(long ts_ms) {
        this.ts_ms = ts_ms;
    }
}
