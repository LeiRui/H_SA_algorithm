package HModel;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/*
  改变数据存储结构对应查询代价建模
 */
public class H_ian {
    public BigDecimal totalRowNumber;

    private int ckn; //排序键的数量

    private int[] ackSeq; // 记录一下
    private List<Column_ian> ACKdist;//按照ACK顺序映射后的排序列的分布参数

    private int qackn;//按照ACK顺序映射后的范围查询的列，从0开始

    private double qck_r1;//范围查询的左端点在该列分布的位置百分比
    private double qck_r2;//范围查询的右端点在该列分布的位置百分比
    private Column_ian.rangeType type;

    private double[] qack_p;//按照ACK顺序映射后,其余ckn-1个点查询值在该列分布的位置百分比，有占位

    public double resP; // 最后的总概率

    /**
     *
     * @param ks
     * @param cf
     * @param pkey
     * @return
     */
    // TODO double int sql
    public String getSql(String ks, String cf, int pkey) {
        String q_format = "select * from "+ks+"."+cf+" where pkey="+pkey;
        for(int i=0;i<ckn;i++) {
            if(i==qackn) {
                switch (type) {
                    case LcRc:
                        q_format += " and ck" + ackSeq[i] + ">=" + qck_r1 + " and ck" + ackSeq[i] + "<=" + qck_r2;
                        break;
                    case LcRo:
                        q_format += " and ck" + ackSeq[i] + ">=" + qck_r1 + " and ck" + ackSeq[i] + "<" + qck_r2;
                        break;
                    case LoRc:
                        q_format += " and ck" + ackSeq[i] + ">" + qck_r1 + " and ck" + ackSeq[i] + "<=" + qck_r2;
                        break;
                    case LoRo:
                        q_format += " and ck" + ackSeq[i] + ">" + qck_r1 + " and ck" + ackSeq[i] + "<" + qck_r2;
                        break;
                    default:
                        break;
                }
            }
            else {
                q_format+=" and ck"+ackSeq[i]+"="+qack_p[i];
            }
        }
        q_format+=" allow filtering;";

        return q_format;
    }

    public H_ian(BigDecimal totalRowNumber, int ckn, List<Column_ian> CKdist,
                 int qckn,
                 double qck_r1_abs, double qck_r2_abs,
                 boolean r1_closed, boolean r2_closed,
                 double[] qck_p_abs,
                 int[] ackSeq){
        this.ackSeq = ackSeq;
        this.totalRowNumber = totalRowNumber;
        this.ckn = ckn;
        Column_ian rqColumn = CKdist.get(qckn-1);
        this.qck_r1=qck_r1_abs * (rqColumn.xmax_ - rqColumn.xmin_) + rqColumn.xmin_;
        this.qck_r2=qck_r2_abs * (rqColumn.xmax_ - rqColumn.xmin_) + rqColumn.xmin_;
        if(r1_closed && r2_closed)
            this.type = Column_ian.rangeType.LcRc;
        else if(r1_closed && !r2_closed)
            this.type = Column_ian.rangeType.LcRo;
        else if(!r1_closed && r2_closed)
            this.type = Column_ian.rangeType.LoRc;
        else
            this.type = Column_ian.rangeType.LoRo;
        ACKdist = new ArrayList<Column_ian>();
        qack_p = new double[ckn];
        for(int i = 0; i < ckn; i++) { //按照ACK顺序映射
            int ackindex = ackSeq[i]-1;
            Column_ian pqColumn = CKdist.get(ackindex);
            ACKdist.add(pqColumn);
            qack_p[i]=qck_p_abs[ackindex]*(pqColumn.xmax_ - pqColumn.xmin_) + pqColumn.xmin_;
            if(ackSeq[i] == qckn) {
                qackn = i; // qackn start from 0 while qckn start from 1
            }
        }
    }

    /**
     * @return HR
     */
    public BigDecimal calculate() {
        resP = 1;
        for (int i = 0; i < qackn; i++) { // qackn是从0开始的，这里刚好表示qckn-1个
            resP *= ACKdist.get(i).getPoint(qack_p[i]);
        }
        switch (type) {
            case LcRc:
                resP *= ACKdist.get(qackn).getBetween(qck_r1, qck_r2, Column_ian.rangeType.LcRc);
                break;
            case LcRo:
                resP *= ACKdist.get(qackn).getBetween(qck_r1, qck_r2, Column_ian.rangeType.LcRo);
                break;
            case LoRc:
                resP *= ACKdist.get(qackn).getBetween(qck_r1, qck_r2, Column_ian.rangeType.LoRc);
                break;
            case LoRo:
                resP *= ACKdist.get(qackn).getBetween(qck_r1, qck_r2, Column_ian.rangeType.LoRo);
                break;
        }
        return totalRowNumber.multiply(new BigDecimal(resP));
        // 这里不能保留2位小数什么的，因为这里如果totalRowNumber给小了，结果HR就是0.02这种，再一保留两位小数就都一样了
    }


    /**
     * @param rowSize
     * @return HB
     */
    public BigDecimal calculate(int rowSize, int blockSize) {
        resP = 1;
        for (int i = 0; i < qackn; i++) { // qackn是从0开始的，这里刚好表示qckn-1个
            resP *= ACKdist.get(i).getPoint(qack_p[i]);
        }
        switch (type) {
            case LcRc:
                resP *= ACKdist.get(qackn).getBetween(qck_r1, qck_r2, Column_ian.rangeType.LcRc);
                break;
            case LcRo:
                resP *= ACKdist.get(qackn).getBetween(qck_r1, qck_r2, Column_ian.rangeType.LcRo);
                break;
            case LoRc:
                resP *= ACKdist.get(qackn).getBetween(qck_r1, qck_r2, Column_ian.rangeType.LoRc);
                break;
            case LoRo:
                resP *= ACKdist.get(qackn).getBetween(qck_r1, qck_r2, Column_ian.rangeType.LoRo);
                break;
        }
        return totalRowNumber.multiply(new BigDecimal(resP)).multiply(new BigDecimal(rowSize))
                .divide(new BigDecimal(blockSize)).setScale(0, RoundingMode.CEILING); // TODO blockSize default 64KB
    }
}
