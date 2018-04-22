package query;

public class AckSeq {
    public int[] ackSeq;
    public int length;
    public AckSeq(int [] ackSeq) {
        this.ackSeq = ackSeq;
        length = ackSeq.length;
    }
    public boolean equals(Object obj){  // 覆写equals，完成对象比较
        if(this==obj){
            return true ;
        }
        if(!(obj instanceof AckSeq)){
            return false ;
        }
        AckSeq p = (AckSeq) obj ;    // 向下转型
        if(p.length != this.length)
            return false;
        for(int i=0;i<length; i++) {
            if(ackSeq[i]!=p.ackSeq[i])
                return false;
        }
        return true;
    }
    public int hashCode(){
        return 0;
    }

    public String toString() {
        String str = "[";
        for(int i=0;i<length;i++) {
            str+=ackSeq[i];
            if(i!= length-1)
                str+="-";
        }
        str+="]";
        return str;
    }
}
