package com.ms.silverking.cloud.storagepolicy;

public class SimpleStoragePolicyCreator {
	public static String	storagePolicyGroupName = "SimplePolicyGroup";
	public static String	storagePolicyName = "SimpleRackPolicy";
	
	public static StoragePolicyGroup createStoragePolicyGroup(int replication) {
		try {
			return new PolicyParser().parsePolicyGroup(createDef(replication), 0);
		} catch (PolicyParseException e) {
			throw new RuntimeException("Panic");
		}
	}
	
	private static String createDef(int replication) {
		String	s;
		
		s = "StoragePolicyGroup:"+ storagePolicyGroupName +" {\n"
		   +"    root    Rack:"+ storagePolicyName +"\n"
		   +"}\n"
		   +"\n"
		   +"Rack:"+ storagePolicyName +" {\n"
		   +"    primary {\n"
           +"        "+ replication +" of Server\n"
		   +"    }\n"
           +"}\n"
		   +"\n";
		return s;
	}
	
	public static void main(String[] args) {
		//System.out.println(createDef(Integer.parseInt(args[0])));
		System.out.println(createStoragePolicyGroup(Integer.parseInt(args[0])));
	}
}
