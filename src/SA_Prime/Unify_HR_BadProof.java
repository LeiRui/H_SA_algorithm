package SA_Prime;

import HModel.Column_ian;
import HModel.H_ian;
import common.Constant;
import query.AckSeq;
import query.RangeQuery;
import query.XAckSeq;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

/**
 * 希望证明的是在查询分流阶段使用最小HR原则不如最小HB的min(max)来的小，因为猜想最小HR的粒度更细，查询分流也更容易聚集到同一个副本，
 * 分散性不如block粒度
 */
@ Deprecated
public class Unify_HR_BadProof {
    public  boolean isDiffReplicated = false; // 统一无异构优化数据存储结构和异构

    // 数据分布参数
    private BigDecimal totalRowNumber;
    private int ckn;
    private List<Column_ian> CKdist;
    // 数据存储参数
    private int rowSize;// unit: byte
    private int blockSize;// unit: byte default: 65536
    // 查询参数
    public List<Integer> queriesPerc;
    private List<RangeQuery> queries;
    // 计算过程中H_ian实例化出的查询语句记录，过后用于cassandra-jdbc-use
    public List<String> sqls;

    // SA
    // X个异构副本组成一个状态解
    // 分流策略：简单的代价最小原则
    public BigDecimal HBCost; // 某个状态解的代价评价：查询按照分流策略分流之后累计的查询代价
    public List<List<Integer>> qchooseX; // queries中每一个查询的路由结果记录，这里是从0开始
    public BigDecimal HRCost_best_bigloop; // SA外圈循环记录每次外圈退温时记忆中保持的最优状态值
    public Set<XAckSeq> ackSeq_best_step1;//记忆性 SA维护的最优状态解集

    public BigDecimal HRCost;
    public BigDecimal HRCost_best;
    public Set<XAckSeq> ackSeq_best_step2;
//    public XAckSeq Output; // 最后的输出

    public int X; // 给定的异构副本数量

    public Unify_HR_BadProof(BigDecimal totalRowNumber, int ckn, List<Column_ian>CKdist,
                 int rowSize, int blockSize,
                 List<Integer> queriesPerc, List<RangeQuery> queries,
                 int X) {
        this.totalRowNumber = totalRowNumber;
        this.ckn = ckn;
        this.CKdist = CKdist;
        this.rowSize = rowSize;
        this.blockSize = blockSize;
        this.queriesPerc = queriesPerc;
        this.queries = queries;
        this.ackSeq_best_step1 = new HashSet();
        this.ackSeq_best_step2 = new HashSet();
        sqls = new ArrayList();
        this.X = X;
        this.qchooseX = new ArrayList();

    }

    /**
     * 关键的状态评价函数
     * 输入：一个状态,X个异构副本组成一个状态解
     * 分流策略：最小HB
     * 状态目标函数值：HCost = max(sum HB)
     * 输出：输入状态的目标函数值HCost
     *
     * @param xackSeq X个异构副本组成一个状态解
     */
    public void calculate(AckSeq[] xackSeq) {
        BigDecimal[] XBload = new BigDecimal[X]; // 用于后面取max(sum HB)
        BigDecimal[] XRload = new BigDecimal[X]; // 用于后面取max(sum HR)
        for(int i=0;i<X;i++) {
            XBload[i] = new BigDecimal("0");
            XRload[i] = new BigDecimal("0");
        }

        qchooseX.clear(); // 每次要清空重新add
        int qnum = queries.size();
        for(int i=0; i<qnum; i++) {// 遍历queries
            RangeQuery q = queries.get(i);
            int qper = queriesPerc.get(i);

            List<Integer> chooseX = new ArrayList(); // 代价一样的副本平分负载
            chooseX.add(0);
            H_ian h = new H_ian(totalRowNumber,ckn,CKdist,
                    q.qckn,q.qck_r1_abs,q.qck_r2_abs,q.r1_closed,q.r2_closed, q.qck_p_abs,
                    xackSeq[0].ackSeq);
            BigDecimal chooseHB = h.calculate(rowSize,blockSize);
            BigDecimal tmpHB;
            if(sqls.size() == i) {
                sqls.add(h.getSql(Constant.ks,Constant.cf));
            }
            for(int j=1;j<X;j++) { // 遍历X个副本，按照最小HB原则对q分流
                h = new H_ian(totalRowNumber,ckn,CKdist,
                        q.qckn,q.qck_r1_abs,q.qck_r2_abs,q.r1_closed,q.r2_closed, q.qck_p_abs,
                        xackSeq[j].ackSeq);
                tmpHB = h.calculate(rowSize,blockSize);
                int res = tmpHB.compareTo(chooseHB);
                if(res == -1) {
                    chooseHB = tmpHB; // note 引用
                    chooseX.clear();
                    chooseX.add(j);
                }
                else if(res == 0) {
                    chooseX.add(j);
                }
            }//X个副本遍历结束，现在已经确定了这个query按照最小HB原则分流到的副本chooseX，以及这个最小HB等于多少
            qchooseX.add(chooseX);
            //接下来更新XBload和XRload
            int chooseNumber = chooseX.size();
            BigDecimal averageQPer = new BigDecimal(qper).divide(new BigDecimal(chooseNumber),10, RoundingMode.HALF_UP);
            BigDecimal averageHB = chooseHB.multiply(averageQPer);
            for(int j=0;j<chooseNumber;j++) {
                int choose = chooseX.get(j);
                XBload[choose]=XBload[choose].add(averageHB); // note 光是.add是不行的 要赋值！

                h = new H_ian(totalRowNumber, ckn, CKdist,
                        q.qckn, q.qck_r1_abs, q.qck_r2_abs, q.r1_closed, q.r2_closed, q.qck_p_abs,
                        xackSeq[choose].ackSeq);
                XRload[choose]=XRload[choose].add(h.calculate().multiply(averageQPer));
            }
        }

        // max(sum HB)的max
        HBCost = XBload[0];
        for(int i=1;i<X;i++) {
            if(XBload[i].compareTo(HBCost) == 1) {
                HBCost = XBload[i];
            }
        }
        // max(sum HR)的max
        HRCost = XRload[0];
        for (int i = 1; i < X; i++) {
            if (XRload[i].compareTo(HRCost) == 1) {
                HRCost = XRload[i];
            }
        }

        //打印结果
        System.out.print(String.format("HB:%.2f HR:%.2f| ",HBCost,HRCost));
        for(int i=0;i<X;i++) {
            System.out.print(String.format("%s:%.2f ",xackSeq[i],XBload[i]));
        }
        for(int i=0;i<qnum; i++) {
            System.out.print(String.format("|q%d->",i+1));
            List<Integer> chooseX = qchooseX.get(i);
            for(int j=0;j<chooseX.size();j++) {
                System.out.printf("R%d",chooseX.get(j)+1);
                if(j!=chooseX.size()-1) {
                    System.out.print(",");
                }
            }
        }
        System.out.println("");
    }


    /**
     * 模拟退火算法
     * 搜索排序键排列，找到使得在给定数据和查询集的条件下，H模型代价最小（已经验证H模型代价和真实查询代价的一致性）
     * 关键环节：
     * 1. 初温，初始解
     * 2. 状态产生函数
     * 3. 状态接受函数
     * 4. 退温函数
     * 5. 抽样稳定准则
     * 6. 收敛准则
     *
     * X个异构副本组成一个状态解
     * 分流策略：简单的代价最小原则
     *
     * SA一步式尚未考虑负载均衡代价：找到HR近似最小的一组解
     * 状态目标值为HR
     *
     */
    public void SA() {
        //确定初温：
        // 随机产生一组状态，确定两两状态间的最大目标值差，然后依据差值，利用一定的函数确定初温
        int setNum = 20;
        List<AckSeq[]> xackSeqList = new ArrayList();
        for(int i=0; i< setNum; i++) {
            AckSeq[] xackSeq = new AckSeq[X];
            shuffle(xackSeq);
            xackSeqList.add(xackSeq);
        }
        BigDecimal maxDeltaR = new BigDecimal("0"); // 两两状态间的最大目标值差
        for(int i=0;i<setNum-1;i++) {
            for(int j =i+1;j<setNum;j++) {
                calculate(xackSeqList.get(i));
                BigDecimal tmp = new BigDecimal(HRCost.toString());
                calculate(xackSeqList.get(j));
                tmp = tmp.subtract(HRCost).abs();
                if(tmp.compareTo(maxDeltaR) == 1) // tmp > maxDeltaB
                    maxDeltaR = tmp;
            }
        }
        double pr = 0.8;
        if(maxDeltaR.compareTo(new BigDecimal("0")) == 0) {
            maxDeltaR = new BigDecimal("0.001");
        }
        BigDecimal t0 = maxDeltaR.negate().divide(new BigDecimal(Math.log(pr)),10, RoundingMode.HALF_UP);
        System.out.println("初温为："+t0);

        //确定初始解
        AckSeq[] currentAckSeq  = new AckSeq[X];
        shuffle(currentAckSeq);
        calculate(currentAckSeq);
        HRCost_best = new BigDecimal(HRCost.toString()); // 至于把currentAckSeq加进Set在后面完成的
        HRCost_best_bigloop = new BigDecimal(HRCost.toString());

        int endCriteria = 20;// 终止准则: BEST SO FAR连续20次退温保持不变
        int endCount = 0;
        int sampleCount = 20;// 抽样稳定准则：20步定步长
        BigDecimal deTemperature = new BigDecimal("0.7"); // 指数退温系数
        while(endCount < endCriteria) {
            // 抽样稳定
            // 记忆性：注意中间最优结果记下来
            for(int sampleloop = 0; sampleloop < sampleCount; sampleloop++) {
                //增加记忆性
                int comp = HRCost.compareTo(HRCost_best);
                if(comp==-1) { //<
                    HRCost_best = HRCost;
                    ackSeq_best_step1.clear();
                    ackSeq_best_step1.add(new XAckSeq(currentAckSeq));
                }
                else if(comp==0) {
                    ackSeq_best_step1.add(new XAckSeq(currentAckSeq));
                }

                //由当前状态产生新状态
                AckSeq[] nextAckSeq = generateNewStateX(currentAckSeq);
                //接受函数接受否
                BigDecimal currentHR = new BigDecimal(HRCost.toString()); // 当前状态的状态值保存在HR
                calculate(nextAckSeq); // HR会被改变
                BigDecimal delta = HRCost.subtract(currentHR); // 新旧状态的目标函数值差
                double threshold;
                if(delta.compareTo(new BigDecimal("0"))!=1) { // <
                    threshold = 1;
                    System.out.println("新状态不比现在状态差");
                }
                else {
                    //threshold = Math.exp(-delta/t0);
                    threshold = Math.exp(delta.negate().divide(t0,10, RoundingMode.HALF_UP).doubleValue()); //TODO
                    System.out.println("新状态比现在状态差");
                }

                if(Math.random() <= threshold) { // 概率接受，替换当前状态
                    currentAckSeq = nextAckSeq;
                    System.out.println("接受新状态");
                    // HR就是现在更新后的HR
                }
                else {// 否则保持当前状态不变
                    HRCost = currentHR;//恢复原来解的状态值
                    System.out.println("维持当前状态不变");
                    // currentAckSeq就是原本的
                }
            }

            if(!HRCost_best.equals(HRCost_best_bigloop)) {
                endCount = 0; // 重新计数
                HRCost_best_bigloop = HRCost_best; // 把当前最小值传递给外圈循环
                System.out.println("【这次退温BEST SO FAR改变】");
            }
            else { // 这次退温后best_so_far和上次比没有改变
                endCount++;
                System.out.println(String.format("【这次退温BEST SO FAR连续%d次没有改变】",endCount));
            }

            //退温
            t0 = t0.multiply(deTemperature);
        }
        //终止 输出结果
    }


    public void combine() {
        System.out.println("----------------------------------------------------");
        // 第一步： SA找到HR代价近似最小的一组解
        SA();
        System.out.println("完成: SA找到HRCost近似最小的"+ackSeq_best_step1.size()+"个近似最优解:");
        for(XAckSeq xackSeq: ackSeq_best_step1) {
            //System.out.print(xackSeq+": ");
            calculate(xackSeq.xackSeq);
//            Output = xackSeq;
        } // 此时结束之后Output以及unify中的所有属性都是ackSeq_best_step2中最后一个元素的计算结果
        System.out.println("目标值HB近似最小为："+HBCost);
        System.out.println("目标值HR近似最小为："+HRCost_best);
//        System.out.println("算法给出一个最后的结果为: ");
//        calculate(Output.xackSeq);
        for(int i=0;i<sqls.size(); i++) {
            System.out.println(sqls.get(i)+":"+queriesPerc.get(i));
        }

    }

    private void shuffle(AckSeq[] xackSeq) {
        if(isDiffReplicated) {// 副本异构
            for (int j = 0; j < X; j++) {
                List<Integer> ackList = new ArrayList();
                xackSeq[j] = new AckSeq(new int[ckn]);
                for (int i = 1; i <= ckn; i++) { // 这里必须从1开始，因为表示ck排序输入参数从1开始
                    ackList.add(i);
                }
                Collections.shuffle(ackList); //JAVA的Collections类中shuffle的用法
                for (int i = 0; i < ckn; i++) {
                    xackSeq[j].ackSeq[i] = ackList.get(i);
                }
            }
        }
        else { // 无异构
            List<Integer> ackList = new ArrayList();
            for (int i = 1; i <= ckn; i++) { // 这里必须从1开始，因为表示ck排序输入参数从1开始
                ackList.add(i);
            }
            Collections.shuffle(ackList); //JAVA的Collections类中shuffle的用法
            for (int j = 0; j < X; j++) {
                xackSeq[j] = new AckSeq(new int[ckn]);
                for (int i = 0; i < ckn; i++) {
                    xackSeq[j].ackSeq[i] = ackList.get(i);
                }
            }
        }
    }


    private AckSeq[] generateNewStateX(AckSeq[] xackSeq) {
        AckSeq[] nextXAckSeq = new AckSeq[X];
        if(isDiffReplicated) { // 副本异构
            for (int i = 0; i < X; i++) {
                nextXAckSeq[i] = new AckSeq(generateNewState(xackSeq[i].ackSeq));
            }
        }
        else { // 无异构
            int[] tmp = generateNewState(xackSeq[0].ackSeq);
            for (int i = 0; i < X; i++) {
                nextXAckSeq[i] = new AckSeq(tmp);
            }
        }
        return nextXAckSeq;
    }

    /**
     * 状态产生函数/邻域函数：由产生候选解的方式和候选解产生的概率分布两部分组成。
     * 出发点：尽可能保证产生的候选解遍布全部解空间
     *
     * @param ackSeq
     */
    private int[] generateNewState(int[] ackSeq) {
        int [] nextAckSeq = new int[ckn];
        for(int i=0;i<ckn;i++) {
            nextAckSeq[i] = ackSeq[i];
        }

        double p_swap = 0.3; // [0,0.3)
        double p_inverse = 0.6; // [0.3,0.6)
        double p_insert = 1; // [0.6,1)

        double r = Math.random();
        if(r<p_swap) {
            generateNewState_swap(nextAckSeq);
        }
        else if(r<p_inverse) {
            generateNewState_inverse(nextAckSeq);
        }
        else {
            generateNewState_insert(nextAckSeq);
        }
        return  nextAckSeq;
    }

    //随机交换状态中两个不同位置的ck
    private void generateNewState_swap(int[] ackSeq){
        Random random = new Random();
        int pos1 = random.nextInt(ckn); // 0~ckn-1
        int pos2 = random.nextInt(ckn);
        while(true) {
            if(pos2!=pos1)
                break;
            else {
                pos2 = random.nextInt(ckn);
            }
        }
        int tmp = ackSeq[pos1];
        ackSeq[pos1] = ackSeq[pos2];
        ackSeq[pos2] = tmp;
    }

    //将两个不同位置间的串逆序
    private void generateNewState_inverse(int[] ackSeq) {
        Random random = new Random();
        int pos1 = random.nextInt(ckn); // 0~ckn-1
        int pos2 = random.nextInt(ckn);
        while(true) {
            if(pos2!=pos1)
                break;
            else {
                pos2 = random.nextInt(ckn);
            }
        }
        if(pos1>pos2) { // make pos1<pos2
            int tmp = pos1;
            pos1 = pos2;
            pos2 = tmp;
        }
        int[] backup = new int[pos2-pos1+1];
        for(int i=pos1; i<= pos2; i++) {
            backup[i-pos1] = ackSeq[i];
        }
        for(int i=pos1; i<=pos2; i++) {
            ackSeq[i] = backup[pos2-i];
        }
    }

    //随机选择某一位置的ck并插入到另一随机位置
    private void generateNewState_insert(int[] ackSeq) {
        Random random = new Random();
        int pos1 = random.nextInt(ckn); // 0~ckn-1
        int pos2 = random.nextInt(ckn);
        while(true) {
            if(pos2!=pos1)
                break;
            else {
                pos2 = random.nextInt(ckn);
            }
        }
        //把pos1位置的ck插入到pos2位置
        int[] add = new int[ckn+1];
        for(int i=0;i<ckn+1;i++) {
            if(i==pos2) {
                add[i] = ackSeq[pos1];
            }
            else if(i>pos2) {
                add[i] = ackSeq[i-1];
            }
            else {
                add[i] = ackSeq[i];
            }
        }
        for(int i = 0; i<ckn; i++) {
            if(pos1<pos2) {
                if (i >= pos1) {
                    ackSeq[i] = add[i + 1];
                } else {
                    ackSeq[i] = add[i];
                }
            }
            else {
                if(i <= pos1) {
                    ackSeq[i] = add[i];
                }
                else {
                    ackSeq[i] = add[i+1];
                }
            }
        }
    }
}
