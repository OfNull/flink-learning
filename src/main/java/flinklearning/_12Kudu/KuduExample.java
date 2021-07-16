package flinklearning._12Kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.LinkedList;
import java.util.List;

public class KuduExample {

    public static void main(String[] args) throws KuduException {
        final String masteradder = "10.130.12.192";

        KuduClient kuduClient = new KuduClient.KuduClientBuilder(masteradder).build();
        crateTable(kuduClient);
//        dropTable(kuduClient);
//        insertRow(kuduClient);
//        queryRow(kuduClient);
        kuduClient.close();

    }

    private static void queryRow(KuduClient kuduClient) throws KuduException {
        KuduTable table = kuduClient.openTable("PERSON");
        KuduScanner.KuduScannerBuilder builder = kuduClient.newScannerBuilder(table);
        /**
         * 设置搜索的条件
         * 如果不设置，则全表扫描
         */
        KuduPredicate predicate = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn("CompanyId"), KuduPredicate.ComparisonOp.EQUAL, 1);
//        builder.addPredicate(predicate);

        KuduScanner scaner = builder.build();

        while (scaner.hasMoreRows()) {
            RowResultIterator iterator = scaner.nextRows();
            while (iterator.hasNext()) {
                RowResult result = iterator.next();
                System.out.print("CompanyId:" + result.getInt("CompanyId") + "   ");
                System.out.print("Name:" + result.getString("Name") + "  ");
                System.out.print("Gender:" + result.getString("Gender") + "    ");
                System.out.print("WorkId:" + result.getInt("WorkId") + "  ");
                System.out.println("Photo:" + result.getString("Photo") + "    ");
            }
        }
        scaner.close();
    }


    private static void insertRow(KuduClient kuduClient) {
        try {
            // 打开表
            KuduTable table = kuduClient.openTable("PERSON");
            // 创建写session,kudu必须通过session写入
            KuduSession session = kuduClient.newSession();
            // 采取Flush方式 手动刷新
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            session.setMutationBufferSpace(3000);
            for (int i = 1; i < 10; i++) {
                Insert insert = table.newInsert();
                // 设置字段内容
                insert.getRow().addInt("CompanyId", i);
                insert.getRow().addInt("WorkId", i);
                insert.getRow().addString("Name", "lisi" + i);
                insert.getRow().addString("Gender", "male");
                insert.getRow().addString("Photo", "person" + i);
                session.flush();
                session.apply(insert);
            }
            session.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表结构
     *
     * @param kuduClient
     */
    private static void crateTable(KuduClient kuduClient) {
        List<ColumnSchema> columns = new LinkedList<ColumnSchema>();
        columns.add(newColumn("CompanyId", Type.INT32, true));
        columns.add(newColumn("WorkId", Type.INT32, false));
        columns.add(newColumn("Name", Type.STRING, false));
        columns.add(newColumn("Gender", Type.STRING, false));
        columns.add(newColumn("Photo", Type.STRING, false));

        Schema schema = new Schema(columns);
        //创建表时提供的所有选项
        CreateTableOptions options = new CreateTableOptions();
        // 设置表的replica备份和分区规则
        List<String> parcols = new LinkedList<String>();
        parcols.add("CompanyId");
        //设置表的备份数
        options.setNumReplicas(0);
        //设置range分区
        options.setRangePartitionColumns(parcols);
        //设置hash分区和数量
        options.addHashPartitions(parcols, 3);

        try {
            kuduClient.createTable("PERSON", schema, options);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    private static ColumnSchema newColumn(String name, Type type, boolean iskey) {
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        column.key(iskey);
        return column.build();
    }


    private static void dropTable(KuduClient kuduClient) throws KuduException {
        kuduClient.deleteTable("PERSON");
    }
}
