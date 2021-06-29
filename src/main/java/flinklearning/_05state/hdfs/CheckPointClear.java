package flinklearning._05state.hdfs;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Hdfs 上检查点清理   判断检查点最后修改修改时间 大于7天就清理掉
 */
public class CheckPointClear {


    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\Users\\Admin\\Desktop\\hadoop\\hadoop-2.7.5");
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(new URI("hdfs://10.130.12.192:9000"), conf, "root");
        //获取文件系统所有文件
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/flinkStandlaloneUat/flink-checkpoints"), true);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        while (listFiles.hasNext()) {
            System.out.println("-----------------分割符号-----------------");
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("是否是文件:" + fileStatus.isFile());
            System.out.println("文件名:" + fileStatus.getPath().getName());
            System.out.println("文件路径:" + fileStatus.getPath());
            System.out.println("访问时间:" + format.format(new Date(fileStatus.getAccessTime())));
            System.out.println("块大小:" + fileStatus.getBlockSize());
            System.out.println("所属组:" + fileStatus.getGroup());
            System.out.println("文件大小:" + fileStatus.getLen());
            System.out.println("修改时间	:" + format.format(new Date(fileStatus.getModificationTime())));
            System.out.println("所有者:" + fileStatus.getOwner());
            System.out.println("副本数:" + fileStatus.getReplication());
            System.out.println("块位置:");
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("-->" + host);
                }
            }
            System.out.println("权限:" + fileStatus.getPermission());
            deleteFile(fs, fileStatus.getPath().getParent().getParent(), fileStatus.getModificationTime());
        }
    }

    private static void deleteFile(FileSystem fs, Path path, long modifyTime) throws IOException {

        DateTime date1 = DateUtil.date(modifyTime);
        DateTime date2 = DateUtil.date(System.currentTimeMillis());
        long day = DateUtil.betweenDay(date1, date2, false);
        if (day > 2) {
            System.out.println("删除文件："+path.getName());
            fs.delete(path, true);
        }


    }
}
