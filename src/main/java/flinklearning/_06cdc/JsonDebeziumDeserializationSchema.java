package flinklearning._06cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.Map;


public class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    private static final String DB = "db";
    private static final String TABLE = "table";

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);

        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        String db = (String) value.getStruct(Envelope.FieldName.SOURCE).get(DB);
        String table = (String) value.getStruct(Envelope.FieldName.SOURCE).get(TABLE);
        long ts_ms = ((Long) value.get(Envelope.FieldName.TIMESTAMP)).longValue();
        BinlogInfo binlogInfo = new BinlogInfo();
        binlogInfo.setDb(db);
        binlogInfo.setTable(table);
        binlogInfo.setTs_ms(ts_ms);
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            Map<String, Object> insert = extractAfterRow(value, valueSchema);
            binlogInfo.setOp(op.code());
            binlogInfo.setAfter(insert);
        } else if (op == Envelope.Operation.DELETE) {
            Map<String, Object> delete = extractBeforeRow(value, valueSchema);
            binlogInfo.setOp(op.code());
            binlogInfo.setBefore(delete);
        } else {
            Map<String, Object> before = extractBeforeRow(value, valueSchema);
            Map<String, Object> after = extractAfterRow(value, valueSchema);
            binlogInfo.setOp(op.code());
            binlogInfo.setBefore(before);
            binlogInfo.setAfter(after);
        }
        out.collect(JSON.toJSONString(binlogInfo));
    }

    private Map<String, Object> extractBeforeRow(Struct value, Schema valueSchema) throws Exception {
        Schema afterSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        Struct after = value.getStruct(Envelope.FieldName.BEFORE);
        return buildMap(afterSchema, after);
    }

    private Map<String, Object> extractAfterRow(Struct value, Schema valueSchema) throws Exception {
        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        return buildMap(afterSchema, after);
    }

    private Map<String, Object> buildMap(Schema afterSchema, Struct after) {
        Map<String, Object> map = new HashMap<>(afterSchema.fields().size());
        if (after == null) {
            return null;
        } else {
            for (Field field : afterSchema.fields()) {
                Object filedValue = after.get(field.name());
                map.put(field.name(), filedValue);
            }
        }
        return map;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
