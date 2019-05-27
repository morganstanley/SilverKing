package com.ms.silverking.cloud.storagepolicy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.cloud.topology.NodeClass;

public class SubPolicy {
    private final ReplicationType       type;
    private final List<SubPolicyMember> members;
    
    public static final int ALL = Integer.MAX_VALUE;
    
    public SubPolicy(ReplicationType type, List<SubPolicyMember> members) {
        this.type = type;
        this.members = members;
        verifyConstraints();
    }
    
    private void verifyConstraints() {
        ensureMemberClassesConsistent();
        ensureBoundMembersIndependent();
        ensureValidBoundOrUnbound();
    }
    
    private void ensureValidBoundOrUnbound() {
        if (members.size() > 0) {
            boolean bound;
            
            bound = members.get(0).isBound();
            for (SubPolicyMember member : members) {
                if (member.isBound() != bound) {
                    throw new RuntimeException("bound/unbound inconsistency among members");
                }
            }
            if (!bound) {
                if (members.size() > 1) {
                    throw new RuntimeException("only a single unbound policy member is allowed");
                }
            }
        }
    }

    private void ensureMemberClassesConsistent() {
        if (members.size() > 0) {
            NodeClass   memberClass;
            
            memberClass = members.get(0).getNodeClassAndStoragePolicyName().getNodeClass();
            for (SubPolicyMember member : members) {
                System.out.println(member +"\n");
                if (member.getNodeClassAndStoragePolicyName().getNodeClass() != memberClass) {
                    System.err.println(member.getNodeClassAndStoragePolicyName().getNodeClass() +" != "+ memberClass);
                    throw new RuntimeException("Inconsistent member node classes");
                }
            }
        }
    }

    private void ensureBoundMembersIndependent() {
        HashSet<String> union;
        int             totalBoundIDs;
        
        union = new HashSet<>();
        totalBoundIDs = 0;
        for (SubPolicyMember member : members) {
            if (member.getBoundIDs() != null) {
                union.addAll(member.getBoundIDs());
                totalBoundIDs += member.getBoundIDs().size();
            }
        }
        if (union.size() != totalBoundIDs) {
            throw new RuntimeException("bound members not independent");
        }
    }
    
    public ReplicationType getReplicationType() {
        return type;
    }
    
    public List<SubPolicyMember> getMembers() {
        return members;
    }
    
    public List<String> getSubPolicyNamesForNodeClass(NodeClass nodeClass, Node child) {
        ArrayList<String>   names;
        
        names = new ArrayList<>();
        for (SubPolicyMember member : members) {
            NodeClassAndStoragePolicyName   n;
            
            n = member.getNodeClassAndStoragePolicyName();
            //System.out.println(n.getNodeClass() +"\t"+ nodeClass +"\t"+ (n.getNodeClass().equals(nodeClass)));
            if (n.getNodeClass().equals(nodeClass)) {
                //System.out.println("\t\t"+ n.getStoragePolicyName());
                if (!member.isBound() || member.getBoundIDs().contains(child.getIDString())) {
                    names.add(n.getStoragePolicyName());
                }
            }
        }
        return names;
    }
    
    /*
    public List<String> getNodeIDs(Topology topology, String parentNodeID) {
        List<String>    nodeIDs;
        
        nodeIDs = new ArrayList<>();
        for (PolicyMember member : members) {
            if (member instanceof BoundPolicyMember) {
                nodeIDs.addAll(((BoundPolicyMember)member).getNodeIDs());
            } else {
                Node    parent;
                
                // find the nodes in the topo ring
                parent = topology.getNodeByID(parentNodeID);
                if (!parent.getNodeClass().equals(nodeClass)) {
                    throw new RuntimeException("nodeClass mismatch");
                } else {
                    nodeIDs.addAll(member.getNodeIDs());
                }
            }
        }
        return nodeIDs;
    }
    */

    @Override
    public String toString() {
        StringBuilder   sb;
        
        sb = new StringBuilder();
        sb.append(type +" {");
        for (SubPolicyMember member : members) {
            sb.append("    "+ member);
        }
        sb.append("}");
        return sb.toString();
    }

    public void toFormattedString(StringBuffer sb) {
        sb.append("\t"+ type +" {\n");
        for (SubPolicyMember member : members) {
            sb.append("\t\t"+ member +"\n");
        }
        sb.append("\t}\n");
    }

    public int getNumReplicas() {
        int total;
        
        total = 0;
        for (SubPolicyMember member : members) {
            total += member.getQuantity();
        }
        return total;
    }
}
