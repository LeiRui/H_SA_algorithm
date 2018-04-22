package SA_Venus;

import HModel.Column_ian;
import query.AckSeq;
import query.RangeQuery;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class CalculateTest {
    public static void main(String[] args) {
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
        queriesPerc.add(1);

        List<RangeQuery> queries = new ArrayList();
        double qck1r1abs = 0.3;
        double qck1r2abs = 0.7;
        double[] qck1pabs = new double[ckn];
        for(int i=0;i<ckn;i++) {
            qck1pabs[i] = 0.5;
        }
        RangeQuery rangeQuery1 = new RangeQuery(1,qck1r1abs,qck1r2abs,true,true,
                qck1pabs);
        RangeQuery rangeQuery2 = new RangeQuery(2,qck1r1abs,qck1r2abs,true,true,
                qck1pabs);
        RangeQuery rangeQuery3 = new RangeQuery(3,qck1r1abs,qck1r2abs,true,true,
                qck1pabs);
        queries.add(rangeQuery1);
        queries.add(rangeQuery2);
        queries.add(rangeQuery3);

        // 构造FindOneBest
        int X=3;
        DiffReplicas_HR findOneBest = new DiffReplicas_HR(totalRowNumber,
                ckn,CKdist,
                rowSize, blockSize,
                queriesPerc,queries,X);
        AckSeq[] xackSeq = new AckSeq[X];
        xackSeq[0]=new AckSeq(new int[]{1,2,3});
        xackSeq[1]=new AckSeq(new int[]{2,3,1});
        xackSeq[2]=new AckSeq(new int[]{3,1,2});
        findOneBest.calculate(xackSeq);

        DiffReplicas_HR findOneBest2 = new DiffReplicas_HR(totalRowNumber,
                ckn,CKdist,
                rowSize, blockSize,
                queriesPerc,queries,X);
        AckSeq[] xackSeq2 = new AckSeq[X];
        xackSeq2[0]=new AckSeq(new int[]{1,2,3});
        xackSeq2[1]=new AckSeq(new int[]{1,2,3});
        xackSeq2[2]=new AckSeq(new int[]{1,2,3});
        findOneBest2.calculate(xackSeq2);



        // 查询参数
        List<Integer> queriesPerc3 = new ArrayList();
        queriesPerc3.add(1);

        rangeQuery1 = new RangeQuery(1,qck1r1abs,qck1r2abs,true,true,
                qck1pabs);
        List<RangeQuery> queries3 = new ArrayList();
        queries3.add(rangeQuery1);

        DiffReplicas_HR findOneBest3 = new DiffReplicas_HR(totalRowNumber,
                ckn,CKdist,
                rowSize, blockSize,
                queriesPerc3,queries3,X);

        AckSeq[] xackSeq3 = new AckSeq[X];
        xackSeq3[0]=new AckSeq(new int[]{1,2,3});
        xackSeq3[1]=new AckSeq(new int[]{2,3,1});
        xackSeq3[2]=new AckSeq(new int[]{3,1,2});
        findOneBest3.calculate(xackSeq3);
    }

}
