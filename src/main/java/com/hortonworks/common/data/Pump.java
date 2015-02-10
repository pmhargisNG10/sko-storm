package com.hortonworks.common.data;

import java.io.Serializable;

/**
 * Created by dpramodv on 12/1/14.
 */
public class Pump implements Serializable {
    int pumpIdNo;
    String pumpId; //GUID so string
    String pumpName;
    String pumpType;

    public int getPumpIdNo() {
        return pumpIdNo;
    }

    public void setPumpIdNo(int pumpIdNo) {
        this.pumpIdNo = pumpIdNo;
    }

    public String getPumpId() {
        return pumpId;
    }

    public void setPumpId(String pumpId) {
        this.pumpId = pumpId;
    }

    public String getPumpName() {
        return pumpName;
    }

    public void setPumpName(String pumpName) {
        this.pumpName = pumpName;
    }

    public String getPumpType() {
        return pumpType;
    }

    public void setPumpType(String pumpType) {
        this.pumpType = pumpType;
    }

    public String toString(){
        return "PumpId=" + pumpId + "|pumpIdNo=" + pumpIdNo + "|pumpName=" + pumpName + "|pumpType=" + pumpType;
    }
}
