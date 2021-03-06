package SA_Sirius;

import HModel.Column_ian;
import query.RangeQuery;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class Test {
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
        int qck1n = 1;
        double qck1r1abs = 0.3;
        double qck1r2abs = 0.7;
        double[] qck1pabs = new double[ckn];
        for(int i=0;i<ckn;i++) {
            //double qpabs = Math.random(); // >=0 and <1
            qck1pabs[i] = 0.5;
        }
        RangeQuery rangeQuery1 = new RangeQuery(1,qck1r1abs,qck1r2abs,true,true,
                qck1pabs);
        RangeQuery rangeQuery2 = new RangeQuery(2,qck1r1abs,qck1r2abs,true,true,
                qck1pabs);
        RangeQuery rangeQuery3 = new RangeQuery(3,qck1r1abs,qck1r2abs,true,true,
                qck1pabs);
        queries.add(rangeQuery1);
        //queries.add(rangeQuery2);
        //queries.add(rangeQuery3);

        // 构造FindOneBest
        FindOneBestOneStep findOneBest = new FindOneBestOneStep(totalRowNumber,
                ckn,CKdist,
                rowSize, blockSize,
                queriesPerc,queries);
//        findOneBest.calculate(new int[]{1,2,3});
//        findOneBest.calculate(new int[]{1,3,2});
//        findOneBest.calculate(new int[]{2,1,3});
//        findOneBest.calculate(new int[]{3,1,2});
//        findOneBest.calculate(new int[]{2,3,1});
//        findOneBest.calculate(new int[]{3,2,1});

        findOneBest.combine();
        List<String> sqls = findOneBest.sqls;
        for(int i=0;i<sqls.size(); i++) {
            System.out.println(sqls.get(i));
        }

    }
}
