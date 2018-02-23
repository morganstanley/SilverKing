package com.ms.silverking.cloud.dht;

import com.ms.silverking.object.ObjectUtil;
import com.ms.silverking.text.ObjectDefParser2;

public class NamespaceServerSideCode {
	private final String	url;
	private final String	putTrigger;
	private final String	retrieveTrigger;
	
    // for parsing only
    static final NamespaceServerSideCode template = new NamespaceServerSideCode("", "", "");
    
    static {
        ObjectDefParser2.addParser(template);
    }

    /**
     * internal use only
     */
    public static void init() {
    }
	
	public NamespaceServerSideCode(String url, String putTrigger, String retrieveTrigger) {
		this.url = url;
		this.putTrigger = putTrigger;
		this.retrieveTrigger = retrieveTrigger;
	}
	
	public String getUrl() {
		return url;
	}

	public String getPutTrigger() {
		return putTrigger;
	}

	public String getRetrieveTrigger() {
		return retrieveTrigger;
	}
	
    @Override
    public String toString() {
        return ObjectDefParser2.objectToString(this);
    }

    public static NamespaceServerSideCode parse(String def) {
        return ObjectDefParser2.parse(NamespaceServerSideCode.class, def);
    }
    
    @Override
    public int hashCode() {
    	return ObjectUtil.hashCode(url) ^ ObjectUtil.hashCode(putTrigger) ^ ObjectUtil.hashCode(retrieveTrigger);
    }
    
    @Override
    public boolean equals(Object o) {
    	NamespaceServerSideCode	other;
    	
    	other = (NamespaceServerSideCode)o;
    	return ObjectUtil.equal(url, other.url) && ObjectUtil.equal(putTrigger, other.putTrigger) && ObjectUtil.equal(retrieveTrigger, other.retrieveTrigger);
    }
    /*
    public static void main(String[] args) {
    	try {
    		NamespaceServerSideCode	o1;
    		NamespaceServerSideCode	o2;
    		NamespaceServerSideCode	o3;
    		
    		o1 = new NamespaceServerSideCode("url", "putTrigger", "retrieveTrigger");
    		System.out.println(o1);
    		o2 = parse(o1.toString());
    		System.out.println(o2);
    		
    		o3 = parse("putTrigger=com.ms.silverking.cloud.skfs.dir.serverside.DirectoryServer,retrieveTrigger=com.ms.silverking.cloud.skfs.dir.serverside.DirectoryServer");
    		System.out.println(o3);
    		
    		NamespaceOptions	no1;
    		NamespaceOptions	no2;
    		
    		no1 = new NamespaceOptions();
    		System.out.println(no1);
    		no2 = NamespaceOptions.parse(no1.toString());
    		System.out.println(no2);
    		
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
    */
}
