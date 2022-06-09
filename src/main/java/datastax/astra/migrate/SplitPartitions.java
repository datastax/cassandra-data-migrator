package datastax.astra.migrate;

import com.datastax.oss.driver.internal.core.metadata.token.RandomToken;
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory;
import org.apache.log4j.Logger;
import scala.math.BigInt;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SplitPartitions {

    public static Logger logger = Logger.getLogger(SplitPartitions.class);
    public final static Long MIN_PARTITION = Long.MIN_VALUE;
    public final static Long MAX_PARTITION  = Long.MAX_VALUE;

    public static final BigInteger MIN_RANDOM = new BigInteger("-1");
    public static final BigInteger MAX_RANDOM = (new BigInteger("2")).pow(127);


    public static void main(String[] args){
        Collection<Partition> partitions = getSubPartitions(new BigInteger("20"), MIN_RANDOM, MAX_RANDOM);
        for(Partition partition: partitions){
            System.out.println(partition);
        }
    }
    public static Collection<Partition> getRandomSubPartitions(BigInteger splitSize, BigInteger min, BigInteger max){

        logger.info("TreadID: " + Thread.currentThread().getId() + " Splitting min: " + min + " max:" + max);
        List<Partition> partitions = getSubPartitions(splitSize,min,max);
        Collections.shuffle(partitions);
        Collections.shuffle(partitions);
        Collections.shuffle(partitions);
        Collections.shuffle(partitions);
        return partitions;
    }
//    private static List<Partition> getSubPartitions(BigInteger splitSize, BigInteger min, BigInteger max){
//        long curMax = min.longValueExact();
//        long partitionSize =  max.subtract(min).divide(splitSize).longValueExact();
//        List<Partition> partitions = new ArrayList<Partition>();
//        if(partitionSize==0){
//            partitionSize=100000;
//        }
//        boolean exausted = false;
//        while(curMax<=max.longValueExact()){
//            long curMin = curMax;
//            long newCurMax = curMin + partitionSize;
//            if (newCurMax < curMax) {
//                newCurMax = max.longValueExact();
//                exausted = true;
//            }
//            if(newCurMax > max.longValueExact()){
//                newCurMax=max.longValueExact();
//                exausted=true;
//            }
//            curMax = newCurMax;
//            partitions.add(new Partition(curMin,curMax));
//            if(exausted){
//                break;
//            }
//        }
//
//        return partitions;
//    }


    private static List<Partition> getSubPartitions(BigInteger splitSize, BigInteger min, BigInteger max){
        BigInteger curMax = new BigInteger(min.toString());
        BigInteger partitionSize =  max.subtract(min).divide(splitSize);
        List<Partition> partitions = new ArrayList<Partition>();
        if(partitionSize.compareTo(new BigInteger("0"))==0){
            partitionSize=new BigInteger("100000");
        }
        boolean exausted = false;
        while(curMax.compareTo(max) <=0){
            BigInteger curMin = new BigInteger(curMax.toString());
            BigInteger newCurMax = curMin.add(partitionSize);
            if (newCurMax.compareTo(curMax) == -1) {
                newCurMax = new BigInteger(max.toString());
                exausted = true;
            }
            if (newCurMax.compareTo(max)==1){
                newCurMax = new BigInteger(max.toString());
                exausted=true;
            }
            curMax = newCurMax;
            partitions.add(new Partition(curMin,curMax));
            if(exausted){
                break;
            }
        }

        return partitions;
    }



    public static class Partition implements Serializable{
        private static final long serialVersionUID = 1L;

        private BigInteger min;
        private BigInteger max;


        public Partition(BigInteger min, BigInteger max){
            this.min = min;
            this.max = max;
        }



        public BigInteger getMin() {
            return min;
        }

        public BigInteger getMax() {
            return max;
        }

        public String toString(){
//            return "--conf spark.migrate.source.minPartition="+ min + " --conf spark.migrate.source.maxPartition=" + max;

            return "select * from field_api.field_users where token(account_id,field_id)>="+ min + " and token(account_id,field_id)<=" + max  + "  and account_id=ee8556f4-9a1a-4c89-ae05-e8105d42ed6f allow  filtering; ";
        }
    }
}