package SA_Venus;

import HModel.Column_ian;
import SA_Sirius.FindOneBest;
import SA_Sirius.FindOneBestOneStep;
import query.RangeQuery;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class BigTest {
    public static void main(String[] args) {
        // 数据分布参数
        //BigDecimal totalRowNumber = new BigDecimal("100000000000000000000");
        BigDecimal totalRowNumber = new BigDecimal("2000000");
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
        int rowSize = 97;
        int blockSize = 65536;

        // 查询参数
        List<Integer> queriesPerc = new ArrayList();
        queriesPerc.add(1);
        queriesPerc.add(2);
        queriesPerc.add(3);
        queriesPerc.add(4);
        queriesPerc.add(4);
        queriesPerc.add(3);
        queriesPerc.add(2);
        queriesPerc.add(2);
        queriesPerc.add(1);
        queriesPerc.add(3);

        int[] qcknGroup = new int[]{1,2,3,4,5,6,7,8,9,10};
        double[] qckr1absGroup = new double[]{0, 0,  0,   0,  0,  0,  0,  0,   0,  0};
        double[] qckr2absGroup = new double[]{1,0.8,0.3,0.9,0.2,0.5, 0.7, 0.5, 0.1,  0.7};

        List<RangeQuery> queries = new ArrayList();
        for(int i=0;i<qcknGroup.length;i++) {
            int qckn = qcknGroup[i];
            double qckr1abs = qckr1absGroup[i];
            double qckr2abs = qckr2absGroup[i];
            double[] qckpabs = new double[ckn];
            for(int j=0;j<ckn;j++) {
                qckpabs[j] = 0.5;
                // TODO 这里会比Math.random更好一点，至少不会100%纯scan blocks
                // TODO 而且其实最关键的是，查询集这样就是确定的，至少不会在跑两次SA之间查询变掉了
            }
            RangeQuery rangeQuery = new RangeQuery(qckn,qckr1abs,qckr2abs,true,true,
                    qckpabs);
            queries.add(rangeQuery);
        }


        // 构造FindOneBest
        /*
        FindOneBest findOneBest = new FindOneBest(totalRowNumber,
                ckn,CKdist,
                rowSize, blockSize,
                queriesPerc,queries);
        findOneBest.combine();
        List<String> sqls = findOneBest.sqls;
        for(int i=0;i<sqls.size(); i++) {
            System.out.println(sqls.get(i));
        }

        FindOneBestOneStep findOneBestOneStep = new FindOneBestOneStep(totalRowNumber,
                ckn,CKdist,
                rowSize, blockSize,
                queriesPerc,queries);
        findOneBest.combine();
        sqls = findOneBest.sqls;
        for(int i=0;i<sqls.size(); i++) {
            System.out.println(sqls.get(i));
        }
        */



//        int X = 3;
//        DiffReplicas_HR diffReplicas_hr = new DiffReplicas_HR(totalRowNumber,
//                ckn,CKdist,
//                rowSize, blockSize,
//                queriesPerc,queries,X);
//        diffReplicas_hr.combine();
//        List<String> sqls2 = diffReplicas_hr.sqls;
//        for(int i=0;i<sqls2.size(); i++) {
//            System.out.println(sqls2.get(i));
//        }

        int X = 3;
        DiffReplicas_HB diffReplicas_hb = new DiffReplicas_HB(totalRowNumber,
                ckn,CKdist,
                rowSize, blockSize,
                queriesPerc,queries,X);
        diffReplicas_hb.combine();
    }
}
