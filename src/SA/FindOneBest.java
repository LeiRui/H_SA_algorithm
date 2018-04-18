package SA;

import HModel.Column_ian;
import HModel.H_ian;
import query.RangeQuery;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * 改进数据存储结构，利用H模型和SA得到近似最优的一种排序键排列解
 */
public class FindOneBest {

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

    // H代价
    public BigDecimal HR;
    public BigDecimal HB;

    // best so far
    //public double HR_best;
    //public int[] ackSeq_bestR;
    public BigDecimal HB_best_bigloop;// 用于收敛准则判断：best_so_far连续20步保持不变
    public BigDecimal HB_best_current;
    public List<int[]> ackSeq_bestB; // 两步式SA的第一步 改进


    public FindOneBest(BigDecimal totalRowNumber, int ckn, List<Column_ian>CKdist,
                       int rowSize, int blockSize,
                       List<Integer> queriesPerc, List<RangeQuery> queries) {
        this.totalRowNumber = totalRowNumber;
        this.ckn = ckn;
        this.CKdist = CKdist;
        this.rowSize = rowSize;
        this.blockSize = blockSize;
        this.queriesPerc = queriesPerc;
        this.queries = queries;
        this.ackSeq_bestB = new ArrayList<>();
    }


    /**
     * 计算一种排序键下的当前代价
     * @param ackSeq 解的编码就是ackSeq，例如[1,3,2,5,4]代表排序键的一种排列[ck1,ck3,ck2,ck5,ck4]
     */
    public void calculate(int[] ackSeq) {
        HR=new BigDecimal("0");
        HB=new BigDecimal("0");

        int qkind = queries.size();
        for(int i=0; i<qkind; i++) {
            RangeQuery q = queries.get(i);
            int qper = queriesPerc.get(i);
            H_ian h = new H_ian(totalRowNumber,ckn,CKdist,
                    q.qckn,q.qck_r1_abs,q.qck_r2_abs,q.r1_closed,q.r2_closed, q.qck_p_abs,
                    ackSeq);
            //System.out.println("q"+(i+1)+"/"+qper+":" +h.getSql("panda","dm1",1));
            HR = HR.add(h.calculate().multiply(new BigDecimal(qper)));
            HB = HB.add(h.calculate(rowSize,blockSize).multiply(new BigDecimal(qper)));
        }
        System.out.print("[");
        for(int i = 0; i < ckn; i++) {
            System.out.print(ackSeq[i]);
            if(i!=ckn-1)
                System.out.print(",");
        }
        System.out.print("]");
        System.out.println(": HR="+HR+", HB="+HB);
        //System.out.println("------");

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
     * 改进SA两步式第一步：找到HB近似最小的一组解
     * 状态目标值为HB
     *
     */
    public void SA_b() {
        //确定初温：
         // 随机产生一组状态，确定两两状态间的最大目标值差，然后依据差值，利用一定的函数确定初温
        int setNum = 20;
        int[][] ackSeqSet = new int[setNum][];
        for(int i=0; i< setNum; i++) {
            ackSeqSet[i] = new int[ckn];
            shuffle(ackSeqSet[i]);
        }
        BigDecimal maxDeltaB = new BigDecimal("0"); // 两两状态间的最大目标值差
        for(int i=0;i<setNum-1;i++) {
            for(int j =i+1;j<setNum;j++) {
                calculate(ackSeqSet[i]);
                BigDecimal tmp = new BigDecimal(HB.toString());
                calculate(ackSeqSet[j]);
                tmp = tmp.subtract(HB).abs();
                if(tmp.compareTo(maxDeltaB) == 1) // tmp > maxDeltaB
                    maxDeltaB = tmp;
            }
        }
        double pr = 0.8;
        BigDecimal t0 = maxDeltaB.negate().divide(new BigDecimal(Math.log(pr)),2, RoundingMode.HALF_UP);

        //确定初始解
        int[] currentAckSeq = new int[ckn];
        shuffle(currentAckSeq);
        calculate(currentAckSeq);
        HB_best_current = HB;

        /*
        HB_best_bigloop = HB;
        HB_best_current = HB;
        ackSeq_bestB.add(currentAckSeq);
        */
        int endCriteria = 20;//终止准则: BEST SO FAR连续20次退温保持不变
        int endCount = 1;
        int sampleCount = 20;// 抽样稳定准则：20步定步长
        BigDecimal deTemperature = new BigDecimal("0.7"); // 指数退温系数
        while(endCount <= endCriteria) {
            // 抽样稳定
            // 记忆性：注意中间最优结果记下来
            for(int sampleloop = 0; sampleloop < sampleCount; sampleloop++) {
                //由当前状态产生新状态
                int[] nextAckSeq = generateNewState(currentAckSeq);
                //接受函数接受否
                 //增加记忆性
                if(HB.compareTo(HB_best_current)==-1) { //<
                    HB_best_current = HB;
                    ackSeq_bestB.clear();
                    ackSeq_bestB.add(currentAckSeq);
                }
                else if(HB==HB_best_current) {
                    ackSeq_bestB.add(currentAckSeq);
                }

                BigDecimal currentHB = new BigDecimal(HB.toString()); // 当前状态的状态值保存在HB
                calculate(nextAckSeq); // HB会被改变
                BigDecimal delta = HB.subtract(currentHB); // 新旧状态的目标函数值差
                double threshold;
                if(delta.compareTo(new BigDecimal("0"))==-1) { // <
                    threshold = 1;
                }
                else {
                    //threshold = Math.exp(-delta/t0);
                    threshold = Math.exp(delta.negate().divide(t0,2, RoundingMode.HALF_UP).doubleValue()); //TODO
                }

                if(Math.random() <= threshold) { // 概率接受，替换当前状态
                    currentAckSeq = nextAckSeq;
                    // HB就是现在更新后的HB
                }
                else {// 否则保持当前状态不变
                    HB = currentHB;//恢复原来解的状态值
                    // currentAckSeq就是原本的
                }
            }

            if(HB_best_current != HB_best_bigloop) {
                endCount = 1; // 重新计数
                HB_best_bigloop = HB_best_current;
            }
            else { // 这次退温后best_so_far和上次比没有改变
                endCount++;
            }

            //退温
            t0 = t0.multiply(deTemperature);
        }
        //终止 输出结果
        System.out.println(HB_best_current);
        System.out.println(HB_best_bigloop);
        System.out.println(ackSeq_bestB.size());

    }

    private void shuffle(int[] ackSeq) {
        List<Integer> ackList = new ArrayList<>();
        for(int i=1; i<=ckn; i++) {
            ackList.add(i);
        }
        Collections.shuffle(ackList); //JAVA的Collections类中shuffle的用法
        for(int i=0;i<ckn;i++) {
            ackSeq[i] = ackList.get(i);
        }
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
