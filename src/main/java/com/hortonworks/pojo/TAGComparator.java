package com.hortonworks.pojo;

import java.util.Comparator;

public class TAGComparator implements Comparator<TAG>{

    public int compare(TAG t1, TAG t2) {
        if(t1.ts > t2.ts){
            return 1;
        } else {
            return -1;
        }
    }
}
