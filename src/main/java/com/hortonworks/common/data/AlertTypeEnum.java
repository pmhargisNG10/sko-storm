package com.hortonworks.common.data;

import org.apache.commons.lang.StringUtils;

/**
 * Created by dpramodv on 11/26/14.
 */
public enum AlertTypeEnum {
    WARNING("WARN"),
    ERROR("ERRR"); // same length strings

    private String type;

    AlertTypeEnum(String type){
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public boolean equals(String fileType) {
        return StringUtils.equals(this.getType(), fileType);
    }
}
