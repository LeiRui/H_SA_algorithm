package query;

public class RangeQuery {
    public int qckn;
    public double qck_r1_abs;
    public double qck_r2_abs;
    public boolean r1_closed;
    public boolean r2_closed;
    public double[] qck_p_abs;
    public RangeQuery(int qckn, double qck_r1_abs, double qck_r2_abs, boolean r1_closed, boolean r2_closed,
                      double[]qck_p_abs) {
        this.qckn = qckn;
        this.qck_r1_abs = qck_r1_abs;
        this.qck_r2_abs = qck_r2_abs;
        this.r1_closed = r1_closed;
        this.r2_closed = r2_closed;
        this.qck_p_abs = qck_p_abs;
    }
}
