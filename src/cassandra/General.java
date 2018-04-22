package cassandra;

import SA_Prime.Unify;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;

public class General {
    public int X;


    // 提前在Cassandra中写入unify算法最后ackSeq_best_step2中的最后一个元素设计给定的数据集，以X张存储在不同结点的表格展现
    // TODO 研究一下多副本的digest request
    // TODO ks.cf是一样的吗？按照partition key的不同分是per ks吗
    // TODO 那大概是还要研究一下哈希算法，确定一下ks.cf的不同pkey分到了哪几个结点，或者像东哥说的那样，实现自己的哈希算法

    public static String node ="127.0.0.1";
    public static String ks;
    public static String[] cfs; // 最普通的无优化的
//    public static String[] cfs = new String[]{"dm3","dm3","dm3"}; // 最普通的无优化的
//    public static String[] cfs = new String[]{"normal1","normal1","normal1"}; // 最普通的无优化的
    //public static String[] cfs = new String[]{"better1","better1","better1"}; // 优化数据存储结构的
    //public static String[] cfs = new String[]{"diff1","diff2","diff3"}; // 异构的仅分流无优化的
    //public static String[] cfs = new String[]{"best1","best2","best3"}; // 异构的优化的

    //实证查询代价
    public double[] parallelCost;

    public List<List<String>> Xsqls;
    public List<List<Double>> Xqperc;

    public int N =100;


    public General(String ks, String[] cfs,
                   List<String> sqls,List<Integer> queriesPerc,List<List<Integer>>qchooseX) {
        this.X = cfs.length;
        Xsqls = new ArrayList();
        Xqperc = new ArrayList();
        for(int i = 0; i < X;i++) {
            Xsqls.add(new ArrayList());
            Xqperc.add(new ArrayList());
        }
        // 接下来对每一个sql按照unity算法分流的结果实例化sqls
        // 在unify.combine之后，unify的qchooseX中就有了每一个查询语句分流的结果
        for(int i = 0; i < X; i++) {
            for(int j =0; j < qchooseX.size(); j++) { // 遍历每一个查询语句
                List<Integer> chooseX = qchooseX.get(j); // 第j条查询语句最终的分流结果
                int chooseNum = chooseX.size(); // 第j条语句被分流到的副本的数目
                for(int z = 0 ; z<chooseNum;z++) {
                    if(chooseX.get(z) == i) { // 都是从0开始
                        Xsqls.get(i).add(String.format(sqls.get(j),ks,cfs[i],1));
                        Xqperc.get(i).add((double)queriesPerc.get(j)/chooseNum);
                        break; // chooseX里不会重复的
                    }
                }
            }
        }
        for(int i=0;i<X;i++) {
            System.out.println("replica "+i+" "+cfs[i]);
            List<String> sql = Xsqls.get(i);
            List<Double> qperc = Xqperc.get(i);
            for(int j=0;j<sql.size();j++) {
                System.out.printf(sql.get(j)+":");
                System.out.printf("%.2f",qperc.get(j));
                System.out.println("");
            }
        }

        parallelCost = new double[X];
    }

    /**
     * 实证测试代价
     */
    public double getFactCost() {
        // 开始
        int N = 100;
        // 线程？
        for(int i=0;i<X;i++) {
            // 分别X并行按照Xsqls合Xqperc执行N批次的查询

            // 假设线程1
            List<String> sqls = Xsqls.get(i); // 分流到第i个副本的查询语句集合
            List<Double> qpercs = Xqperc.get(i); // 分流到第i个副本的查询语句比重
            int qnum = sqls.size();
            List<Integer> executePercsNum = new ArrayList<Integer>();
            for (int j = 0; j < qnum; j++) {
                executePercsNum.add((int) Math.round(qpercs.get(j) * N));
            }
            // TODO 算法计算的精确结果有一点出入,但是这样先精确乘以N之后再取整的误差会比先对qperc取整再乘以N的误差要小
            // 甚至当qperc是0.33这种时，若先对qperc上取整1，结果执行100条再平均到一条耗时，但是实际算法算的是0.33*该条HB代价，1和0.33差很多
            // 0.33*100=33，得到33条总代价之后平均到0.33条实际代价，这样和算法期待稍微接近了

            Cluster cluster = Cluster.builder().addContactPoint(node).build();
            Session session = cluster.connect();

            // warm up
            for (int j = 0; j < 20; j++) {
                for (int z = 0; z < qnum; z++) {
                    ResultSet rs = session.execute(sqls.get(z));
                    int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
                }
            }
            // 实证查询
            List<Double> resRecord = new ArrayList<Double>();
            double sumup = 0;

            long elapsed = System.nanoTime();
            for (int j = 0; j < qnum; j++) {
                String sql = sqls.get(j);
                int executeNum = executePercsNum.get(j);
                for (int z = 0; z < executeNum; z++) {
                    ResultSet rs = session.execute(sql);
                    int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
                }
            }
            elapsed = System.nanoTime() - elapsed;
            double cost = elapsed / (double) Math.pow(10, 6); // unit: ms
            parallelCost[i] = cost / N;
            System.out.printf("replica %d:%fms\n", i, parallelCost[i]);
            session.close();
            cluster.close();
        }

        // 取所有线程最后一个执行完的耗时，也就是最大耗时作为本次测试在给定数据、查询和算法优化的存储结构下的实证代价
        double maxCost = parallelCost[0];
        for(int i = 1; i < X;i++) {
            if(parallelCost[i]>maxCost) {
                maxCost = parallelCost[i];
            }
        }
        System.out.printf("Cost=%fms\n",maxCost);
        return maxCost;
    }
}
