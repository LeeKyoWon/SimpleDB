package com.ll;

import java.lang.reflect.Field;

public class Util {
    static Class<?>[] getConstructorParamTypes(Field[] fields) {
        Class<?>[] paramTypes = new Class[fields.length];
        for(int i=0; i< fields.length; i++) {
            paramTypes[i] = fields[i].getType();
        }
        return paramTypes;
    }
}
