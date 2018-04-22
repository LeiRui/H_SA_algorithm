package SA_Prime;

import HModel.Column_ian;
import cassandra.General;
import query.AckSeq;
import query.RangeQuery;
import query.XAckSeq;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/*
 在ckn=3，X=3时全解空间大小是3!*3!*3!=6*6*6=216个（认为X个副本之间是有排列的话，
 虽然算法中判断两个解是否相等的时候是不管排列顺序的@XAckSeq
 也就是说216个里会有重复，或者用Set<XAckSeq>来自动去重吧
 216/A33=216/6=36种？


2,1,2
1,2,2是一样的

1，2，2
2，1，1

1,2,3,
1,3,4
1,4,4
1,4,3

 */
public class FullCompareTest {
    public static void main(String[] args) {
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileOutputStream("FullCompareTest.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


        // 数据分布参数
        BigDecimal totalRowNumber = new BigDecimal("1000000");
        int ckn=3;
        List<Column_ian> CKdist = new ArrayList<Column_ian>();
        double step = 1;
        List<Double> x = new ArrayList<Double>();
        for(int i = 1; i<=101; i++) {
            x.add((double)i);
        }
        List<Integer> y = new ArrayList<Integer>();
        for(int i = 1; i<=100; i++) {
            y.add(1);
        }
        Column_ian ck1 = new Column_ian(step, x, y);
        Column_ian ck2 = new Column_ian(step, x, y);
        Column_ian ck3 = new Column_ian(step, x, y);
        CKdist.add(ck1);
        CKdist.add(ck2);
        CKdist.add(ck3);

        // 数据存储参数
        int rowSize = 24;
        int blockSize = 65536;

        // 查询参数
        List<Integer> queriesPerc = new ArrayList();
        queriesPerc.add(1);
        queriesPerc.add(1);
        queriesPerc.add(3);

        List<RangeQuery> queries = new ArrayList();
        int qck1n = 1;
//        double qck1r1abs = 0.3;
//        double qck1r2abs = 0.7;
        double[] qck1pabs = new double[ckn];
        for(int i=0;i<ckn;i++) {
            //double qpabs = Math.random(); // >=0 and <1
            qck1pabs[i] = 0.5;
        }
        RangeQuery rangeQuery1 = new RangeQuery(1,0.3,0.7,true,true,
                qck1pabs);
        RangeQuery rangeQuery2 = new RangeQuery(2,0.2,0.6,true,true,
                qck1pabs);
        RangeQuery rangeQuery3 = new RangeQuery(3,0.4,1,true,true,
                qck1pabs);
        queries.add(rangeQuery1);
        queries.add(rangeQuery2);
        queries.add(rangeQuery3);

        int X = 3;
        Unify_fixed unify = new Unify_fixed(totalRowNumber,
                ckn, CKdist,
                rowSize, blockSize,
                queriesPerc, queries, X);
        unify.isDiffReplicated = true;
        //unify.combine(); // 顺便得到了sqls format

///////////////////////////////////////////////////////////////////////////////////////////////

        String[] cfs = new String[]{"dm1","dm2","dm3","dm4","dm5","dm6"}; // cassandra中实际存储的表名
        List<int[]> cfs_map = new ArrayList<int[]>(); // 对应存储的表结构或者说ck排序
        cfs_map.add(new int[]{1,2,3});
        cfs_map.add(new int[]{1,3,2});
        cfs_map.add(new int[]{2,1,3});
        cfs_map.add(new int[]{3,1,2});
        cfs_map.add(new int[]{2,3,1});
        cfs_map.add(new int[]{3,2,1});
        Set<XAckSeq> removeRepeat = new HashSet<XAckSeq>();
        for(int i=0;i<1;i++) {
            for(int j=0;j<1;j++) { // 换成j=i好像也可以达到去重的效果？
                for(int z=0;z<1;z++) { // 换成z=j好像也可以达到去重的效果？
                    XAckSeq xAckSeq = new XAckSeq(new AckSeq[]{new AckSeq(cfs_map.get(i)),
                            new AckSeq(cfs_map.get(j)),
                            new AckSeq(cfs_map.get(z))});
                    System.out.println(xAckSeq);
                    if (!removeRepeat.contains(xAckSeq)) {
                        removeRepeat.add(xAckSeq);
                        unify.calculate(xAckSeq.xackSeq);//得到算法得出的该状态下按照最小HB原则的分流结果和状态目标函数值
                        pw.write(xAckSeq.toString()+","+unify.HBCost+","+unify.HRCost);
                        String[] state = new String[]{cfs[i], cfs[j], cfs[z]};
                        General general = new General("panda", state,
                                unify.sqls, queriesPerc, unify.qchooseX);
                        pw.write(","+general.getFactCost()+"\n");
                    }

                }
            }
        }
        pw.close();
    }
}
