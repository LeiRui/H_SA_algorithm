package query;

public class XAckSeq {
    public AckSeq[] xackSeq;
    public int X;
    public XAckSeq(AckSeq[] xackSeq) {
        this.xackSeq = xackSeq;
        X = xackSeq.length;
    }
    public boolean equals(Object obj){  // 覆写equals，完成对象比较
        if(this==obj){
            return true ;
        }
        if(!(obj instanceof XAckSeq)){
            return false ;
        }
        XAckSeq p = (XAckSeq) obj ;    // 向下转型
        if(p.X!=this.X)
            return false;
        boolean [] a = new boolean[X]; // default false
        boolean [] b = new boolean[X]; // default false
        for(int i=0;i<X; i++) {
            for(int j=0; j<X;j++) {
                if (xackSeq[i].equals(p.xackSeq[j]) && a[j]==false) {
                    a[j] = true;
                    b[i] = true;
                    break;
                }
            }
            if(b[i]==false)
                return false;
        }
        return true;
    }
    public int hashCode(){
        return 0;
    }

    public String toString() {
        String str = "{";
        for(int i=0;i<X;i++) {
            str+=xackSeq[i].toString();
            if(i!= X-1)
                str+=",";
        }
        str+="}";
        return str;
    }
}
