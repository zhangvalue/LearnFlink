/**
 * @ Author zhangsf
 * @CreateTime 2019/12/27 - 11:10 AM
 */
package flink.kmeans;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * K-Means 是迭代的聚类算法，初始设置K个聚类中心
 * 在每一次迭代过程中，算法计算每个数据点到每个聚类中心的欧式距离
 * 每个点被分配到它最近的聚类中心
 * 随后每个聚类中心被移动到所有被分配的点
 * 移动的聚类中心被分配到下一次迭代
 * 算法在固定次数的迭代之后终止(在本实现中，参数设置)
 * 或者聚类中心在迭代中不在移动
 * 本项目是工作在二维平面的数据点上
 * 它计算分配给集群中心的数据点
 * 每个数据点都使用其所属的最终集群(中心)的id进行注释。
 * For example <code>"1.2 2.3\n5.3 7.2\n"</code> gives two data points (x=1.2, y=2.3) and (x=5.3, y=7.2).
 * <li>Cluster centers are represented by an integer id and a point value.<br>
 * For example <code>"1 6.2 3.2\n2 2.9 5.7\n"</code> gives two centers (id=1, x=6.2, y=3.2) and (id=2, x=2.9, y=5.7).
 * </ul>
 * <p>Usage: KMeans --points &lt;path&gt; --centroids &lt;path&gt; --output &lt;path&gt; --iterations &lt;n&gt;</code><br>
 * 如果没有参数提供，项目使用默认数据运行聚类程序并迭代10次。
 **/
public class KmeanTask {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        // 获取输入的数据点和聚类中心，如果路径中有数据就读文件，否则取默认数据
        DataSet<Point> points = getPointDataSet(params, env);
        DataSet<Centroid> centroids = getCentroidDataSet(params, env);

        // 设置 K-Means算法的迭代次数
        IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("iterations", 10));

        DataSet<Centroid> newCentroids = points
                // 为每个点(point)计算最近的聚类中心
                .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                // 每个聚类中心的点坐标的计数和求和
                .map(new CountAppender())
                .groupBy(0)
                .reduce(new CentroidAccumulator())
                // 从点计数和坐标，计算新的聚类中心
                .map(new CentroidAverager());

        // 将新的中心点放到下一次迭代中,closeWith代表最后一次迭代
        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);
        // 最后将分类和聚类的点生成元组
        DataSet<Tuple2<Integer, Point>> clusteredPoints = points
                // 将point分派到最后聚类中
                .map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

        // 将结果集存到csv文件中或者打印到控制台
        if (params.has("output")) {
            clusteredPoints.writeAsCsv(params.get("output"), "\n", StringUtils.SPACE);

            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("KMeans Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            clusteredPoints.print();
        }
    }

    // *************************************************************************
    //     数据源读取 (数据点和聚类中心)
    // *************************************************************************

    private static DataSet<Centroid> getCentroidDataSet(ParameterTool params, ExecutionEnvironment env) {
        DataSet<Centroid> centroids;
        if (params.has("centroids")) {
            centroids = env.readCsvFile(params.get("centroids"))
                    .fieldDelimiter(StringUtils.SPACE)
                    .pojoType(Centroid.class, "id", "x", "y");
        } else {
            System.out.println("执行 K-Means 用默认的中心数据集合.");
            System.out.println("Use --centroids to specify file input.");
            centroids = KMeansData.getDefaultCentroidDataSet(env);
        }
        return centroids;
    }

    private static DataSet<Point> getPointDataSet(ParameterTool params, ExecutionEnvironment env) {
        DataSet<Point> points;
        if (params.has("points")) {
            // read points from CSV file
            points = env.readCsvFile(params.get("points"))
                    .fieldDelimiter(StringUtils.SPACE)
                    .pojoType(Point.class, "x", "y");
        } else {
            System.out.println("Executing K-Means example with default point data set.");
            System.out.println("Use --points to specify file input.");
            points = KMeansData.getDefaultPointDataSet(env);
        }
        return points;
    }

    // *************************************************************************
    //    数据类型，POJO内部类
    // *************************************************************************

    /**
     * 简单的二维点.
     */
    public static class Point implements Serializable {

        public double x, y;

        public Point() {}

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public Point add(Point other) {
            x += other.x;
            y += other.y;
            return this;
        }

        public Point div(long val) {
            x /= val;
            y /= val;
            return this;
        }

        public double euclideanDistance(Point other) {
            return Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
        }


        public void clear() {
            x = y = 0.0;
        }

        @Override
        public String toString() {
            return x + StringUtils.SPACE + y;
        }
    }

    /**
     * 简单的二维中心，包括ID的点
     */
    public static class Centroid extends Point {

        public int id;

        public Centroid() {}

        public Centroid(int id, double x, double y) {
            super(x, y);
            this.id = id;
        }

        public Centroid(int id, Point p) {
            super(p.x, p.y);
            this.id = id;
        }

        @Override
        public String toString() {
            return id + " " + super.toString();
        }
    }

    // *************************************************************************
    //     自定义函数
    // *************************************************************************

    /** 从数据点确定最近的聚类中心. */
    @FunctionAnnotation.ForwardedFields("*->1")
    public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {
        private List<Centroid> centroids;

        /** 从广播变量中读取聚类中心值到集合中. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Point> map(Point p) throws Exception {

            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            // 检查所有的聚类中心
            for (Centroid centroid : centroids) {
                // 计算每个点与聚类中心的距离（欧式距离）
                double distance = p.euclideanDistance(centroid);

                // 满足条件更新最近的聚类中心Id
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.id;
                }
            }

            // 生成一个包含聚类中心id和数据点的元组tuple.
            return new Tuple2<>(closestCentroidId, p);
        }
    }

    /** 向tupel2追加计数变量. */
    @FunctionAnnotation.ForwardedFields("f0;f1")
    public static final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer/*id*/, Point, Long/*1L*/> map(Tuple2<Integer, Point> t) {
            return new Tuple3<>(t.f0, t.f1, 1L);
        }
    }

    /** 求同一个类所有点的x,y坐标总数和计数点坐标. */
    //@FunctionAnnotation.ForwardedFields("0")
    public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
            return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
        }
    }

    /** 从坐标和点的个数计算新的聚类中心. */
    //@FunctionAnnotation.ForwardedFields("0->id")
    public static final class CentroidAverager implements MapFunction<Tuple3<Integer/*id*/, Point/*累加的坐标点*/, Long/*个数*/>, Centroid> {

        @Override
        public Centroid map(Tuple3<Integer, Point, Long> value) {
            return new Centroid(value.f0, value.f1.div(value.f2));
        }
    }

    /**
     * 缺省的数据准备
     */
    public static class KMeansData {
        // We have the data as object arrays so that we can also generate Scala Data Sources from it.
        public static final Object[][] CENTROIDS = new Object[][] {
                new Object[] {1, -31.85, -44.77},
                new Object[]{2, 35.16, 17.46},
                new Object[]{3, -5.16, 21.93},
                new Object[]{4, -24.06, 6.81}
        };

        public static final Object[][] POINTS = new Object[][] {
                new Object[] {-14.22, -48.01},
                new Object[] {-22.78, 37.10},
                new Object[] {56.18, -42.99},
                new Object[] {35.04, 50.29},
                new Object[] {-9.53, -46.26},
                new Object[] {-34.35, 48.25},
                new Object[] {55.82, -57.49},
                new Object[] {21.03, 54.64},
                new Object[] {-13.63, -42.26},
                new Object[] {-36.57, 32.63},
                new Object[] {50.65, -52.40},
                new Object[] {24.48, 34.04},
                new Object[] {-2.69, -36.02},
                new Object[] {-38.80, 36.58},
                new Object[] {24.00, -53.74},
                new Object[] {32.41, 24.96},
                new Object[] {-4.32, -56.92},
                new Object[] {-22.68, 29.42},
                new Object[] {59.02, -39.56},
                new Object[] {24.47, 45.07},
                new Object[] {5.23, -41.20},
                new Object[] {-23.00, 38.15},
                new Object[] {44.55, -51.50},
                new Object[] {14.62, 59.06},
                new Object[] {7.41, -56.05},
                new Object[] {-26.63, 28.97},
                new Object[] {47.37, -44.72},
                new Object[] {29.07, 51.06},
                new Object[] {0.59, -31.89},
                new Object[] {-39.09, 20.78},
                new Object[] {42.97, -48.98},
                new Object[] {34.36, 49.08},
                new Object[] {-21.91, -49.01},
                new Object[] {-46.68, 46.04},
                new Object[] {48.52, -43.67},
                new Object[] {30.05, 49.25},
                new Object[] {4.03, -43.56},
                new Object[] {-37.85, 41.72},
                new Object[] {38.24, -48.32},
                new Object[] {20.83, 57.85}
        };

        public static DataSet<Centroid> getDefaultCentroidDataSet(ExecutionEnvironment env) {
            List<Centroid> centroidList = new LinkedList<Centroid>();
            for (Object[] centroid : CENTROIDS) {
                centroidList.add(
                        new Centroid((Integer) centroid[0], (Double) centroid[1], (Double) centroid[2]));
            }
            return env.fromCollection(centroidList);
        }

        public static DataSet<Point> getDefaultPointDataSet(ExecutionEnvironment env) {
            List<Point> pointList = new LinkedList<Point>();
            for (Object[] point : POINTS) {
                pointList.add(new Point((Double) point[0], (Double) point[1]));
            }
            return env.fromCollection(pointList);
        }
    }
}

