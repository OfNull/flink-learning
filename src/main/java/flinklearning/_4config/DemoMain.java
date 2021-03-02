package flinklearning._4config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class DemoMain {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterToolEnvironmentUtils.createParameterTool(args);
        env.getConfig().setGlobalJobParameters(parameterTool);

        System.out.println(parameterTool.get("url"));
        System.out.println(parameterTool.get("username"));
        System.out.println(parameterTool.get("password"));
    }
}
