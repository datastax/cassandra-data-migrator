package datastax.astra.migrate;

import org.apache.commons.lang.ObjectUtils;

public class SrcDestKey {

    private String srcId;
    private String astraId;
    private String metaId;

    public SrcDestKey(String srcId, String astraId, String metaId){
        this.srcId=srcId;
        this.astraId=astraId;
        this.metaId=metaId;
    }

    public String getSrcId() {
        return srcId;
    }

    public String getAstraId() {
        return astraId;
    }

    public String getMetaId() {
        return metaId;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(srcId + "%" + astraId);
    }

    @Override
    public boolean equals(Object obj) {

        if(!(obj instanceof SrcDestKey)){
            return false;
        }
        SrcDestKey otherObj  = (SrcDestKey)obj;
        if(srcId.equals(otherObj.srcId)){
            if(astraId==null && otherObj.astraId==null) {
                return true;
            }
            return astraId!=null && astraId.equals(otherObj.astraId);
        }
        return false;
    }
}
