package cassandra;

import java.util.ArrayList;
import java.util.List;

/*
 用Unify算法的结果作为这里的输入参数，实证代价
 */
public class GeneralTest {
    public static void main(String[] args) {
        List<String> sqls = new ArrayList<String>();
        sqls.add("select * from %s.%s where pkey=%d and ck1>=1 and ck1<=101 and ck2=51 and ck3=51 allow filtering;");
        sqls.add("select * from %s.%s where pkey=%d and ck1=51 and ck2>=1 and ck2<=101 and ck3=51 allow filtering;");
        sqls.add("select * from %s.%s where pkey=%d and ck1=51 and ck2=51 and ck3>=1 and ck3<=101 allow filtering;");

        List<Integer> queriesPerc = new ArrayList<Integer>();
        queriesPerc.add(1);
        queriesPerc.add(1);
        queriesPerc.add(3);

        //////////////////////////////////////////////
//
//        int X=3;
//
//        String[] cfs = new String[]{"dm4","dm3","dm3"};
//
//        List<List<Integer>> qchooseX = new ArrayList<List<Integer>>();
//        for(int i=0;i<X;i++) {
//            qchooseX.add(new ArrayList<Integer>());
//        }
//        qchooseX.get(0).add(0);// TODO:这里qchooseX从0开始！
//        qchooseX.get(0).add(1);// TODO:这里qchooseX从0开始！
//        qchooseX.get(0).add(2);// TODO:这里qchooseX从0开始！
//        qchooseX.get(1).add(0);// TODO:这里qchooseX从0开始！
//        qchooseX.get(2).add(1);// TODO:这里qchooseX从0开始！
//        qchooseX.get(2).add(2);// TODO:这里qchooseX从0开始！
//
//
//        General general = new General("panda", cfs,
//                sqls,queriesPerc,qchooseX);
//        general.getFactCost();
        ///////////////////////////////////////////////
        int X=3;

        String[] cfs = new String[]{"dm2","dm6","dm6"};

        List<List<Integer>> qchooseX = new ArrayList<List<Integer>>();
        for(int i=0;i<X;i++) {
            qchooseX.add(new ArrayList<Integer>());
        }
        qchooseX.get(0).add(1);// TODO:这里qchooseX从0开始！
        qchooseX.get(0).add(2);// TODO:这里qchooseX从0开始！
        qchooseX.get(1).add(0);// TODO:这里qchooseX从0开始！
        qchooseX.get(2).add(0);// TODO:这里qchooseX从0开始！


        General general = new General("panda", cfs,
                sqls,queriesPerc,qchooseX);
        general.getFactCost();
    }
}
