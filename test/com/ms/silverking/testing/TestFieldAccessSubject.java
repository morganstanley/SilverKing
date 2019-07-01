package com.ms.silverking.testing;

public class TestFieldAccessSubject {
    private String _String;
    private byte _byte;
    private short _short;
    private int  _int;
    private long _long;
    private float _float;
    private double _double;
    private char _char;
    private boolean _boolean;
     
    public static final String  stringFieldName = "_String";
    public static final String  byteFieldName = "_byte";
    public static final String  shortFieldName = "_short";
    public static final String  intFieldName = "_int";
    public static final String  longFieldName = "_long";
    public static final String  floatFieldName = "_float";
    public static final String  doubleFieldName = "_double";
    public static final String  charFieldName = "_char";
    public static final String  booleanFieldName = "_boolean";
     
    public TestFieldAccessSubject() {
    }
     
    public String toString() {
        return _String + _byte + _short + _int + _long + _float + _double + _char + _boolean;
    }
}
