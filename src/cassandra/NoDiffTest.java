package cassandra;

import SA_Prime.Unify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import jnr.ffi.annotations.In;


/*
  测试无副本异构情况下，算法通过改进数据存储结构能够带来的查询代价的提升
 */
@Deprecated
public class NoDiffTest {

    // Unity包含算法所需的所有输入和输出
    public Unify unify;
    public int X;

    // 提前在Cassandra中写入unify算法最后ackSeq_best_step2中的最后一个元素设计给定的数据集，以X张存储在不同结点的表格展现
    // TODO 研究一下多副本的digest request
    // TODO ks.cf是一样的吗？按照partition key的不同分是per ks吗
    // TODO 那大概是还要研究一下哈希算法，确定一下ks.cf的不同pkey分到了哪几个结点，或者像东哥说的那样，实现自己的哈希算法
    public int[] pkey; // 不同pkey模拟代表了一张表的副本
    public static String[] nodes = new String[]{"127.0.0.1","127.0.0.1","127.0.0.1"} ;

    //实证查询代价
    public double[] parallelCost;

    public List<List<String>> Xsqls;
    public List<List<Integer>> Xqperc;
    // TODO 这里在把perc平分之后用四舍五入取整，回合算法计算的精确结果有一点出入
    // TODO unify中：BigDecimal averageQPer = new BigDecimal(qper).divide(new BigDecimal(chooseNumber),10, RoundingMode.HALF_UP


    public NoDiffTest(Unify unify) {
        this.unify = unify;
        this.X = unify.X;
        // 接下来对每一个sql按照unity算法分流的结果实例化sqls
        // 在unify.combine之后，unify的qchooseX中就有了每一个查询语句分流的结果
        pkey = new int[X];
        for(int i = 0; i < X; i++) {
            pkey[i] = i+1; // TODO 希望分别按顺序对应着unify最终优化出来的

            for(int j =0; j < unify.qchooseX.size(); j++) { // 遍历每一个查询语句
                List<Integer> chooseX = unify.qchooseX.get(j); // 第j条查询语句最终的分流结果
                int chooseNum = chooseX.size(); // 第j条语句被分流到的副本的数目
                for(int z = 0 ; z<chooseNum;z++) {
                    if(chooseX.get(z) == i) { // 都是从0开始
                        Xsqls.get(i).add(String.format(unify.sqls.get(j),pkey[i]));
                        Xqperc.get(i).add((int)Math.round((double)unify.queriesPerc.get(j)/chooseNum));
                        // TODO 这里在把perc平分之后用四舍五入取整，回合算法计算的精确结果有一点出入
                    }
                }
            }
        }
        parallelCost = new double[X];
    }

    /**
     * 实证测试代价
     */
    public double getFactCost(Unify unify) {
        // 开始
        int N = 100;
        // 线程？
        for(int i=0;i<X;i++) {
            // 分别X并行按照Xsqls合Xqperc执行N批次的查询
            // 假设线程1
            List<String> sqls = Xsqls.get(i); // 分流到第i个副本的查询语句集合
            List<Integer> qpercs = Xqperc.get(i); // 分流到第i个副本的查询语句比重
            int qnum = sqls.size();

            Cluster cluster = Cluster.builder().addContactPoint(nodes[i]).build();
            Session session = cluster.connect();

            // warm up
            for (int j = 0; j < 20; j++) {
                for(int z = 0; z<qnum; z++) {
                    ResultSet rs = session.execute(sqls.get(z));
                    int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
                }
            }
            // 实证查询
            List<Double> resRecord = new ArrayList<Double>();
            double sumup = 0;
            for (int m = 0; m < N; m++) {
                long elapsed = System.nanoTime();
                for(int j=0;j<qnum;j++) {
                    int qper = qpercs.get(j);
                    for (int z = 0; z < qper; z++) { //in a batch
                        ResultSet rs = session.execute(sqls.get(j));
                        int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
                    }
                }
                elapsed = System.nanoTime() - elapsed;
                double cost = elapsed / (double) Math.pow(10, 6); // unit: ms
                resRecord.add(cost);
                sumup += cost;
            }
            sumup /= N;
            parallelCost[i] = sumup; // TODO 如何返回主线程

            System.out.print(String.format(", Real-Mean:%8.3f", sumup));
            // 统计min,80th percentile,95th percentile,max
            Collections.sort(resRecord);
            int eighty_index = (int) Math.ceil(N * 0.8);
            int ninety_five_index = (int) Math.ceil(N * 0.95);
            System.out.println(String.format(", min:%8.3f, 80th percentile:%8.3f, 95th percentile:%8.3f, max:%8.3f"
                    ,resRecord.get(0)
                    ,resRecord.get(eighty_index - 1)
                    ,resRecord.get(ninety_five_index - 1)
                    ,resRecord.get(resRecord.size() - 1)));
        }

        // 取所有线程最后一个执行完的耗时，也就是最大耗时作为本次测试在给定数据、查询和算法优化的存储结构下的实证代价
        double maxCost = parallelCost[0];
        for(int i = 1; i < X;i++) {
            if(parallelCost[i]>maxCost) {
                maxCost = parallelCost[i];
            }
        }
        return maxCost;
    }








}
