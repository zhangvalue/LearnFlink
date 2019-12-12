/**
 * @ Author zhangsf
 * @CreateTime 2019/12/11 - 9:27 PM
 */
package flink.learn;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用ParameterTool工具获取端口值
 */
public class StreamingWCJavaApp5 {
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
        text.flatMap(new MyFlatMapFunction()).keyBy("word")
                .timeWindow(Time.seconds(5))
                .sum("count").print().setParallelism(1);

        //step4 execute
        try {
            env.execute("StreamingWCJavaApp");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static class MyFlatMapFunction extends RichFlatMapFunction<String, WC> {
        @Override
        public void flatMap(String value, Collector<WC> collector) throws Exception {
            String[] tokens = value.toLowerCase().split(",");
            for (String token : tokens ) {
                if (token.length()>0){
                    collector.collect(new WC(token,1));
                }
            }
        }
    }
    public static class WC{
        /**
         * word
         */
        private String word;

        public WC() {
        }

        @Override
        public String  toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

        public WC(String word, int count) {
            this.word = word;
            this.count = count;
        }

        /**
         * count
         */
        private int count = 0;

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }
}
