/**
 * @ Author zhangsf
 * @CreateTime 2019/12/11 - 9:27 PM
 */
package flink.learn;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用ParameterTool工具获取端口值
 */
public class StreamingWCJavaApp2 {
    public static void main(String[] args) {
        //获取参数
        int port=0;

       try{
           ParameterTool tool = ParameterTool.fromArgs(args);
           port=tool.getInt("port");
       }
       catch (Exception e){
           System.err.print("端口未设置，使用默认端口9999");
           port=9999;
       }

        // set up the streaming execution environment
        //step1  ：获取执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //step2 :读取数据
        DataStreamSource<String> text = env.socketTextStream("localhost",port);
        //step3: transform
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = value.toLowerCase().split(",");
                for (String token : tokens ) {
                    if (token.length()>0){
                        collector.collect(new Tuple2<String, Integer>(token,1));
                    }
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print().setParallelism(1);

        //step4 execute
        try {
            env.execute("StreamingWCJavaApp");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
