package com.ms.silverking.collection;

import java.util.List;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.text.StringUtil;

@OmitGeneration
public class Pair<T1,T2> extends TupleBase {
	private final T1    v1;
    private final T2    v2;
    
	private static final long serialVersionUID = 4842255422592843585L;
	
	private static final int	SIZE = 2;    
    
    public Pair(T1 v1, T2 v2) {
    	super(SIZE);
        this.v1 = v1;
        this.v2 = v2;
    }
    
    public static <T1,T2> Pair<T1,T2> of(T1 v1, T2 v2) {
        return new Pair<>(v1, v2);
    }
    
    public T1 getV1() {
        return v1;
    }

    public T2 getV2() {
        return v2;
    }
    
    @Override
    public int hashCode() {
        return v1.hashCode() ^ v2.hashCode();
    }
    
    @Override
    public boolean equals(Object other) {
    	if (other == this) {
    		return true;
    	} else {
	        Pair<T1,T2> oPair;
	        
	        oPair = (Pair<T1,T2>)other;
	        return v1.equals(oPair.v1) && v2.equals(oPair.v2);
    	}
    }
    
    @Override 
    public String toString() {
    	return StringUtil.nullSafeToString(v1) + ":"+ StringUtil.nullSafeToString(v2);
    }
    
    
    public static <T1,T2> Pair<T1,T2> parse(String def, String pattern, String... typeNames) {
    	List<Object>	l;
    	
    	l = TupleUtil.parse(def, pattern, SIZE, typeNames);
    	return new Pair(l.get(0), l.get(1));
    }
}
