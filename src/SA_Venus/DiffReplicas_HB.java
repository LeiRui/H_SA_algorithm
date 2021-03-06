package SA_Venus;

import HModel.Column_ian;
import HModel.H_ian;
import common.Constant;
import query.AckSeq;
import query.RangeQuery;
import query.XAckSeq;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

/*
  与DiffReplicas_HR的区别在：这里SA的状态的目标函数值用HB而非HR，两步式，最后从SA得到的HB近似最小集合中挑出最小HR的解集合
 */
public class DiffReplicas_HB {
    // 数据分布参数
    private BigDecimal totalRowNumber;
    private int ckn;
    private List<Column_ian> CKdist;
    // 数据存储参数
    private int rowSize;// unit: byte
    private int blockSize;// unit: byte default: 65536
    // 查询参数
    private List<Integer> queriesPerc;
    private List<RangeQuery> queries;
    // 计算过程中H_ian实例化出的查询语句记录，过后用于cassandra-jdbc-use
    public List<String> sqls;

    // SA
    // X个异构副本组成一个状态解
    // 分流策略：简单的代价最小原则
    public double[]  Xload; // 某状态下X个副本按照分流策略规划到的负载数
    private double aveLoad; // sumPerc/x
    private double BalanceThreshold; // 负载不均衡指标的阈值
    private double BalanceThresholdBaseline; // 负载不均衡指标基准
    public BigDecimal HR; // 某个状态解的代价评价：查询按照分流策略分流之后累计的查询代价
    public BigDecimal HB; // 某个状态解的代价评价：查询按照分流策略分流之后累计的查询代价

    public BigDecimal HB_best;// SA每次内圈得到新状态之后维护的最优状态值
    public BigDecimal HR_best;// SA每次内圈得到新状态之后维护的最优状态值
    public Set<XAckSeq> ackSeq_bestB;//记忆性 SA维护的最优状态解集
    public Set<XAckSeq> ackSeq_bestB_balanced;//记忆性 SA维护的最优状态解集
    public Set<XAckSeq> ackSeq_bestR;//记忆性 SA维护的最优状态解集
    public BigDecimal HB_best_bigloop; // SA外圈循环记录每次外圈退温时记忆中保持的最优状态值

    public int X; // 给定的异构副本数量

    public DiffReplicas_HB(BigDecimal totalRowNumber, int ckn, List<Column_ian>CKdist,
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
        this.ackSeq_bestB = new HashSet();
        this.ackSeq_bestR = new HashSet();
        sqls = new ArrayList();

        this.X = X;

        double sumPerc = 0;
        int qkinds = queriesPerc.size();
        for(int i=0; i<qkinds;i++) {
            sumPerc += queriesPerc.get(i);
        }
        aveLoad = sumPerc/X;
        BalanceThresholdBaseline = (1-aveLoad)*(1-aveLoad)/X; // 不均衡极端1,0,0,0,..时候的方差的10%

    }

    /**
     * 关键的状态评价函数
     * 输入：一个状态，
     * 输出：查询按照分流策略分流后的总查询代价HR/HB，以及每个副本分别承载的查询负载Xload
     *
     * X个异构副本组成一个状态解
     * 分流策略：简单的代价最小原则
     *
     * @param xackSeq X个异构副本组成一个状态解
     */
    public void calculate(AckSeq[] xackSeq) {
        HR=new BigDecimal("0"); // 和初始化  站在查询角度的H代价
        HB=new BigDecimal("0"); // 和初始化  站在查询角度的H代价
        Xload = new double[X];         // 和初始化  站在结点角度的负载均衡代价

        int qkind = queries.size();
        for(int i=0; i<qkind; i++) {
            RangeQuery q = queries.get(i);// 遍历每一类查询
            int qper = queriesPerc.get(i);// 遍历每一类查询

            //遍历给定的状态中X个异构副本，根据H代价和代价最小的分流策略，确定当前查询分流的副本，
            // 从而累加这个查询的查询代价（记得乘以比重），以及累加这个副本分流到的查询负载
            List<Integer> chooseX = new ArrayList(); // 当两个副本代价一样的时候，随机选一个，这两个副本平分这个负载
            chooseX.add(0);
            H_ian h = new H_ian(totalRowNumber,ckn,CKdist,
                    q.qckn,q.qck_r1_abs,q.qck_r2_abs,q.r1_closed,q.r2_closed, q.qck_p_abs,
                    xackSeq[0].ackSeq);
            BigDecimal chooseHB = h.calculate(rowSize,blockSize); // TODO 暂时就用一步式 比较代价用HB，计算最终结果代价也把HR计算出来
            BigDecimal tmpHB; // TODO 暂时就用一步式 比较代价用HB，计算最终结果代价也把HR计算出来
            if(sqls.size() == i) {
                sqls.add(h.getSql(Constant.ks,Constant.cf));
            }
            for(int j=1;j<X;j++) {
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
            }
            HB = HB.add(chooseHB.multiply(new BigDecimal(qper)));
            h = new H_ian(totalRowNumber,ckn,CKdist,
                    q.qckn,q.qck_r1_abs,q.qck_r2_abs,q.r1_closed,q.r2_closed, q.qck_p_abs,
                    xackSeq[chooseX.get(0)].ackSeq);//计算最终结果代价也把HB计算出来
            HR = HR.add(h.calculate().multiply(new BigDecimal(qper)));
            int chooseNumber = chooseX.size();
            for(int j=0;j<chooseNumber;j++) {
                Xload[chooseX.get(j)] += (double) qper / chooseNumber;//平分这个查询负载
            }
        }

        //打印结果
        for(int i=0;i<X;i++) {
            System.out.print(String.format(" %s:%5.2f ",xackSeq[i],Xload[i]));
        }
        System.out.println(":HR="+HR.setScale(3, RoundingMode.HALF_UP)+", HB="+HB);
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
    public void SA_b() {
        //确定初温：
        // 随机产生一组状态，确定两两状态间的最大目标值差，然后依据差值，利用一定的函数确定初温
        int setNum = 20;
        List<AckSeq[]> xackSeqList = new ArrayList();
        for(int i=0; i< setNum; i++) {
            AckSeq[] xackSeq = new AckSeq[X];
            shuffle(xackSeq);
            xackSeqList.add(xackSeq);
        }
        BigDecimal maxDeltaB = new BigDecimal("0"); // 两两状态间的最大目标值差
        for(int i=0;i<setNum-1;i++) {
            for(int j =i+1;j<setNum;j++) {
                calculate(xackSeqList.get(i));
                BigDecimal tmp = new BigDecimal(HB.toString());
                calculate(xackSeqList.get(j));
                tmp = tmp.subtract(HB).abs();
                if(tmp.compareTo(maxDeltaB) == 1) // tmp > maxDeltaB
                    maxDeltaB = tmp;
            }
        }
        double pr = 0.8;
        if(maxDeltaB.compareTo(new BigDecimal("0")) == 0) {
            maxDeltaB = new BigDecimal("0.001");
        }
        BigDecimal t0 = maxDeltaB.negate().divide(new BigDecimal(Math.log(pr)),10, RoundingMode.HALF_UP);
        System.out.println("初温为："+t0);

        //确定初始解
        AckSeq[] currentAckSeq  = new AckSeq[X];
        shuffle(currentAckSeq);
        calculate(currentAckSeq);
        HB_best = new BigDecimal(HB.toString()); // 至于把currentAckSeq加进Set在后面完成的
        HB_best_bigloop = new BigDecimal(HB.toString());

        int endCriteria = 20;// 终止准则: BEST SO FAR连续20次退温保持不变
        int endCount = 0;
        int sampleCount = 20;// 抽样稳定准则：20步定步长
        BigDecimal deTemperature = new BigDecimal("0.7"); // 指数退温系数
        while(endCount < endCriteria) {
            // 抽样稳定
            // 记忆性：注意中间最优结果记下来
            for(int sampleloop = 0; sampleloop < sampleCount; sampleloop++) {
                //增加记忆性
                int comp = HB.compareTo(HB_best);
                if(comp==-1) { //<
                    HB_best = HB;
                    ackSeq_bestB.clear();
                    ackSeq_bestB.add(new XAckSeq(currentAckSeq));
                }
                else if(comp==0) {
                    ackSeq_bestB.add(new XAckSeq(currentAckSeq));
                }

                //由当前状态产生新状态
                AckSeq[] nextAckSeq = generateNewStateX(currentAckSeq);
                //接受函数接受否
                BigDecimal currentHB = new BigDecimal(HB.toString()); // 当前状态的状态值保存在HB
                calculate(nextAckSeq); // HB会被改变
                BigDecimal delta = HB.subtract(currentHB); // 新旧状态的目标函数值差
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
                    HB = currentHB;//恢复原来解的状态值
                    System.out.println("维持当前状态不变");
                    // currentAckSeq就是原本的
                }
            }

            if(!HB_best.equals(HB_best_bigloop)) {
                endCount = 0; // 重新计数
                HB_best_bigloop = HB_best; // 把当前最小值传递给外圈循环
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

    private void shuffle(AckSeq[] xackSeq) {
        for(int j=0;j<X;j++) {
            List<Integer> ackList = new ArrayList();
            xackSeq[j]=new AckSeq(new int[ckn]);
            for (int i = 1; i <= ckn; i++) { // 这里必须从1开始，因为表示ck排序输入参数从1开始
                ackList.add(i);
            }
            Collections.shuffle(ackList); //JAVA的Collections类中shuffle的用法
            for(int i=0;i<ckn;i++) {
                xackSeq[j].ackSeq[i] = ackList.get(i);
            }
        }
    }


    private AckSeq[] generateNewStateX(AckSeq[] xackSeq) {
        AckSeq[] nextXAckSeq = new AckSeq[X];
        for(int i=0;i<X;i++) {
            nextXAckSeq[i] = new AckSeq(generateNewState(xackSeq[i].ackSeq));
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

    /**
     * 改进SA两步式第二步：在HB近似最小的一组解中找到HR最小的解
     * 状态目标值：HR
     */
    public void SA_r() {
        Iterator iterator = ackSeq_bestB_balanced.iterator();
        if (iterator.hasNext()) {
            XAckSeq xack = (XAckSeq) iterator.next();
            calculate(xack.xackSeq);
            HR_best = HR;
            ackSeq_bestR.add(xack);
        }
        while (iterator.hasNext()) {
            XAckSeq xack = (XAckSeq) iterator.next();
            calculate(xack.xackSeq);
            int res = HR.compareTo(HR_best);
            if (res == -1) { //<
                HR_best = HR;
                ackSeq_bestR.clear();
                ackSeq_bestR.add(xack);
            } else if (res == 0) {
                ackSeq_bestR.add(xack);
            }
        }
    }

    public void combine() {
        System.out.println("----------------------------------------------------");
        // 第一步： SA找到HB代价近似最小的一组解
        SA_b();
        System.out.println("根据目标值HB找到"+ackSeq_bestB.size()+"个近似最优解:");
        // 第二步： 计算这组解的查询负载的不均衡指标，过滤掉其中特别不均衡的解
        ackSeq_bestB_balanced = new HashSet();
        int loose = 0;
        while(ackSeq_bestB_balanced.size()==0) { // 一开始阈值比较严格，如果严格的阈值找不到一个解，再放松阈值直到找到一个
            loose++;
            BalanceThreshold = BalanceThresholdBaseline*0.1*loose;
            for (XAckSeq xackSeq : ackSeq_bestB) {
                System.out.print(xackSeq + ": ");
                calculate(xackSeq.xackSeq);
                if (BalanceCheck(Xload)) { // 负载均衡检查通过
                    ackSeq_bestB_balanced.add(xackSeq);
                }
            }
        }
        System.out.println("----------------------------------------------------");
        System.out.println("过滤掉负载不均衡指标超过阈值的解后，剩下"+ackSeq_bestB_balanced.size()+"个近似最优解:");
        SA_r();
        System.out.println("----------------------------------------------------");
        System.out.println("再从中取出HR最小的解，共"+ackSeq_bestR.size()+"个近似最优解:");
        System.out.println("目标值HB近似最小为："+HB_best);
        System.out.println("目标值HR近似最小为："+HR_best);
        for(XAckSeq xackSeq: ackSeq_bestR) {
            System.out.print(xackSeq+": ");
            calculate(xackSeq.xackSeq);
        }

        for(int i=0;i<sqls.size(); i++) {
            System.out.println(sqls.get(i));
        }

    }

    private boolean BalanceCheck(double[] Xload) {
        double variance = 0;
        for(int i=0; i < X;i++){
            variance+=(Xload[i]-aveLoad)*(Xload[i]-aveLoad);
        }
        variance /= X;
        if(variance>BalanceThreshold)
            return false; // 判定为不均衡
        else
            return true; // 判定为可以接受的均衡
    }
}
