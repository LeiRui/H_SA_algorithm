package SA;

import HModel.Column_ian;
import query.AckSeq;
import query.RangeQuery;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BigTest {
    public static void main(String[] args) {
        // 数据分布参数
        //long totalRowNumber = 100000000;
        //BigDecimal totalRowNumber = new BigDecimal("100000000000000000000");
        BigDecimal totalRowNumber = new BigDecimal("1000000");
        int ckn=10; //10! = 3628800

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
        for (int i = 0; i < ckn; i++) {
            Column_ian ck = new Column_ian(step,x,y);
            CKdist.add(ck);
        }

        // 数据存储参数
        int rowSize = 24;
        int blockSize = 65536;

        // 查询参数
        List<Integer> queriesPerc = new ArrayList<>();
        queriesPerc.add(2);
        queriesPerc.add(7);
        queriesPerc.add(5);
        queriesPerc.add(3);

        List<RangeQuery> queries = new ArrayList<>();
        int qck1n = 1;
        double qck1r1abs = 0;
        double qck1r2abs = 1;
        double[] qck1pabs = new double[ckn];
        for(int i=0;i<ckn;i++) {
            //qck1pabs[i] = Math.random(); // >=0 and <1
            qck1pabs[i] = 0.5;
        }
        RangeQuery rangeQuery1 = new RangeQuery(qck1n,qck1r1abs,qck1r2abs,true,true,
                qck1pabs);
        queries.add(rangeQuery1);

        int qck2n = 2;
        double qck2r1abs = 0;
        double qck2r2abs = 0.5;
        double[] qck2pabs = new double[ckn];
        for(int i=0;i<ckn;i++) {
            qck2pabs[i] = Math.random(); // >=0 and <1
        }
        RangeQuery rangeQuery2 = new RangeQuery(qck2n,qck2r1abs,qck2r2abs,true,true,
                qck2pabs);
        queries.add(rangeQuery2);

        int qck3n = 7;
        double qck3r1abs = 0;
        double qck3r2abs = 0.5;
        double[] qck3pabs = new double[ckn];
        for(int i=0;i<ckn;i++) {
            qck3pabs[i] = Math.random(); // >=0 and <1
        }
        RangeQuery rangeQuery3 = new RangeQuery(qck3n,qck3r1abs,qck3r2abs,true,true,
                qck3pabs);
        queries.add(rangeQuery3);

        int qck4n = 10;
        double qck4r1abs = 0;
        double qck4r2abs = 0.4;
        double[] qck4pabs = new double[ckn];
        for(int i=0;i<ckn;i++) {
            qck4pabs[i] = Math.random(); // >=0 and <1
        }
        RangeQuery rangeQuery4 = new RangeQuery(qck4n,qck4r1abs,qck4r2abs,true,true,
                qck4pabs);
        queries.add(rangeQuery4);

        // 构造FindOneBest
        FindOneBest findOneBest = new FindOneBest(totalRowNumber,
                ckn,CKdist,
                rowSize, blockSize,
                queriesPerc,queries);

        findOneBest.combine();

    }
}
