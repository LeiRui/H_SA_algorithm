package HModel;

import java.util.List;

/*
 new idea
 */
public class Column_ian {
    public double step_;
    private List<Double> x_;
    private List<Integer> y_;
    public double xmin_;
    public double xmax_;
    private int sum_;


    /**
     * 输入直方频数统计信息
     * @param x  note: x比y多一项，因为y是每段的值，x是标点，x最后一个是取不到的终止位
     * @param y
     */
    public Column_ian(double step, List<Double> x, List<Integer> y) {
        this.step_ = step;
        this.x_ = x;
        this.y_ = y;
        this.xmin_= x.get(0);
        this.xmax_ = x.get(x.size()-1); // TODO 这个x的结束位真是有些些麻烦
        sum_=0;
        for(int num: y_) {
            sum_+=num;
        }
    }

    public double getPoint(double p) {
        int index = 0;
        for(; index < x_.size()-1; index++) {
            if(p>=x_.get(index) && p<x_.get(index + 1)) {
                break;
            }
        }
        int number = (int)Math.round((x_.get(index+1)-x_.get(index))/step_);
        // TODO mind if query some like 1.1 while there is no 1.1 then actually it is fast, so do not test [point query] nonexist
        // TODO mind when step is very small
        return (double)(y_.get(index))/(sum_*number);
    }

    public enum rangeType {
        LcRc,LcRo,LoRc,LoRo
    }

    public double getBetween(double r1, double r2, rangeType type) {
        double res = 0;

        for(int i = 0; i < x_.size()-1; i++) {
            int number = (int)Math.round((x_.get(i+1)-x_.get(i))/step_);
            double pos = x_.get(i);
            while(pos < x_.get(i+1)) {
                if(isIn(pos, r1, r2, type)) {
                    res += (double)(y_.get(i))/(sum_*number);
                }
                pos += step_;
            }
        }
        return res;
    }

    private boolean isIn(double pos, double r1, double r2, rangeType type) {
        switch (type) {
            case LcRc:
                if(pos >= r1 && pos <= r2)
                    return true;
                else
                    return false;
            case LcRo:
                if(pos >= r1 && pos < r2)
                    return true;
                else
                    return false;
            case LoRc:
                if(pos > r1 && pos <= r2)
                    return true;
                else
                    return false;
            case LoRo:
                if(pos > r1 && pos < r2)
                    return true;
                else
                    return false;
            default:
                return false;
        }
    }


}
