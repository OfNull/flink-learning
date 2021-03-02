package flinklearning._4config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

/**
 * 带环境的 配置读取工具类
 */
public class ParameterToolEnvironmentUtils {
    public static final String defaultConfigFileName = "flink.properties";
    public static final String environmentFileNameTemplate = "flink-%s.properties";
    public static final String ENV_ACTIVE = "env.active";


    public static ParameterTool createParameterTool(String[] args) throws IOException {
        //系统环境参数 -Dyy=xx
        ParameterTool systemProperties = ParameterTool.fromSystemProperties();
        //运行参数 main参数  flink自己数据需要 - 或者 -- 开头做key 例如 -name tom or --name tom
        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        //默认配置文件
        ParameterTool defaultPropertiesFile = ParameterTool.fromPropertiesFile(Thread.currentThread().getContextClassLoader().getResourceAsStream(defaultConfigFileName));
        //按照优先级获取有效环境值
        String envActiveValue = getEnvActiveValue(systemProperties, fromArgs, defaultPropertiesFile);
        String currentEnvFileName = String.format(environmentFileNameTemplate, envActiveValue);
        //读取合并环境参数
        ParameterTool currentEnvPropertiesFile = ParameterTool.fromPropertiesFile(Thread.currentThread().getContextClassLoader().getResourceAsStream(currentEnvFileName));
        ParameterTool parameterTool = currentEnvPropertiesFile.mergeWith(defaultPropertiesFile).mergeWith(fromArgs).mergeWith(systemProperties);
        return parameterTool;
    }

    /**
     * 按照优先级获取有效环境值
     *
     * @return
     */
    public static String getEnvActiveValue(ParameterTool systemProperties, ParameterTool fromArgs, ParameterTool defaultPropertiesFile) {
        //选择参数环境
        String env;
        if (systemProperties.has(ENV_ACTIVE)) {
            env = systemProperties.get(ENV_ACTIVE);
        } else if (fromArgs.has(ENV_ACTIVE)) {
            env = fromArgs.get(ENV_ACTIVE);
        } else if (defaultPropertiesFile.has(ENV_ACTIVE)) {
            env = defaultPropertiesFile.get(ENV_ACTIVE);
        } else {
            //如果没有配置抛出异常
            throw new IllegalArgumentException(String.format("%s does not exist！ Please set up the environment. for example： flink.properties Add configuration env.active = dev", ENV_ACTIVE));
        }
        return env;
    }

    public static void main(String[] args) {
        try {
            ParameterTool parameterTool = ParameterToolEnvironmentUtils.createParameterTool(args);
            System.out.println("贫贱之知不相移,糟糠之妻不下康");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
