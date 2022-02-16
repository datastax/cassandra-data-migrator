package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.internal.core.data.DefaultTupleValue;
import org.javatuples.Tuple;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;

public class MigrateDataType {
    Class typeClass = Object.class;
    List<Class> subTypes = new ArrayList<Class>();

    public MigrateDataType(String dataType){
        if(dataType.contains("%")){
            int count =1 ;
            for(String type: dataType.split("%")){
                if(count==1){
                    typeClass = getType(Integer.parseInt(type));
                }else{
                    subTypes.add(getType(Integer.parseInt(type)));
                }
                count++;
            }
        }else {
            int type = Integer.parseInt(dataType);
            typeClass = getType(type);
        }
    }

    public boolean diff(Object source, Object astra){
        if(source==null && astra==null){
            return false;
        }if(source==null && astra!=null){
            return true;
        }else if(source!=null && astra==null){
            return true;
        }


        return !source.equals(astra);
    }
    private Class getType(int type){
        switch(type) {
            case 0:
                return String.class;
            case 1:
                return Integer.class;

            case 2:
                return Long.class;

            case 3:
                return Double.class;

            case 4:
                return Instant.class;

            case 5:
                return Map.class;
            case 6:
                return List.class;
            case 7:
                return ByteBuffer.class;
            case 8:
                return Set.class;
            case 9:
                return UUID.class;
            case 10:
                return Boolean.class;
            case 11:
                return TupleValue.class;
        }

        return Object.class;
    }

}
