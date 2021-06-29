package flinklearning._08Kafka;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RangeFlinkKafkaPartitioner<T> extends FlinkKafkaPartitioner<T> {
    private AtomicInteger counter;
    private int parallelInstanceId;
    private int parallelInstances;

    @Override
    public void open(int parallelInstanceId, int parallelInstances) {
        Preconditions.checkArgument(parallelInstanceId >= 0, "Id of this subtask cannot be negative.");
        Preconditions.checkArgument(parallelInstances > 0, "Number of subtasks must be larger than 0.");
        this.counter = new AtomicInteger(0);
        this.parallelInstanceId = parallelInstanceId;
        this.parallelInstances = parallelInstances;
    }

    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        int[] targetPartitions = computePartitions(partitions);
        int length = targetPartitions.length;
        if (length == 1) {
            return targetPartitions[0];
        } else {
            int count = nextValue();
            return targetPartitions[length % count];
        }
    }

    protected int[] computePartitions(int[] partitions) {
        if (partitions.length == parallelInstances) {
            return new int[]{partitions[parallelInstanceId % partitions.length]};
        } else if (partitions.length > parallelInstances) {
            //并行数小于分区数
            int m = (int) Math.ceil((float) partitions.length / parallelInstances);
            List<Integer> parallelPartitionList = new ArrayList<>();
            for (int i = 0; i < m; i++) {
                int partitionIndex = parallelInstanceId + (i * parallelInstances);
                if ((partitionIndex + 1) <= partitions.length) {
                    parallelPartitionList.add(partitions[partitionIndex]);
                }
            }
            System.out.println(String.format("当前子任务：【%s m=%s】 arrs【%s】", parallelInstanceId, m, JSON.toJSONString(parallelPartitionList)));
            int[] parallelPartitionArr = new int[parallelPartitionList.size()];
            for (int i = 0; i < parallelPartitionList.size(); i++) {
                parallelPartitionArr[i] = parallelPartitionList.get(i);
            }
            return parallelPartitionArr;

        } else {
            //并行数大于分区数 todo 待实现
            return new int[]{partitions[parallelInstanceId % partitions.length]};
        }
    }


    private int nextValue() {
//        if (counter == null) {
//            counter = new AtomicInteger(0);
//        }
        return counter.getAndIncrement();
    }


    public static void main(String[] args) {
        int[] partitions = new int[18];
        for (int i = 0; i < 18; i++) {
            partitions[i] = i;
        }

        int maxPaller = 9;
        for (int i = 0; i < maxPaller; i++) {
            RangeFlinkKafkaPartitioner partitioner = new RangeFlinkKafkaPartitioner();
            partitioner.open(i, maxPaller);
            partitioner.computePartitions(partitions);
        }
    }

}
