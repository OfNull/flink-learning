package flinklearning._4config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

/**
 * 配置读取工具类
 */
public class ParameterToolUtils {
    public static ParameterTool createParameterTool(String[] args) throws IOException {
        //-D 参数覆盖 args参数 覆盖 配置文件参数
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(Thread.currentThread().getContextClassLoader().getResourceAsStream("application.properties"))
                .mergeWith(ParameterTool.fromArgs(args))
                .mergeWith(ParameterTool.fromSystemProperties());
        return parameterTool;
    }

    public static void main(String[] args) {
        try {
            ParameterTool parameterTool = ParameterToolUtils.createParameterTool(args);
            System.out.println("贫贱之知不相移,糟糠之妻不下康");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
