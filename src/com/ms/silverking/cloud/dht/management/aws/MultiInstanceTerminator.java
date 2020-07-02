package com.ms.silverking.cloud.dht.management.aws;

import static com.ms.silverking.cloud.dht.management.aws.Util.deleteKeyPair;
import static com.ms.silverking.cloud.dht.management.aws.Util.findRunningInstancesWithKeyPair;
import static com.ms.silverking.cloud.dht.management.aws.Util.getIds;
import static com.ms.silverking.cloud.dht.management.aws.Util.getInstanceIds;
import static com.ms.silverking.cloud.dht.management.aws.Util.getIps;
import static com.ms.silverking.cloud.dht.management.aws.Util.print;
import static com.ms.silverking.cloud.dht.management.aws.Util.printDone;
import static com.ms.silverking.cloud.dht.management.aws.Util.printNoDot;

import java.util.List;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;

import static com.ms.silverking.cloud.dht.management.aws.Util.getMyIp;
import static com.ms.silverking.cloud.dht.management.aws.Util.getUniqueKeyPairName;

public class MultiInstanceTerminator {

  private final AmazonEC2 ec2;
  private final String keyPairName;
  private List<Instance> instances;

  public MultiInstanceTerminator(AmazonEC2 ec2, String launchHostIp) {
    this.ec2 = ec2;
    this.keyPairName = getUniqueKeyPairName(launchHostIp);

    instances = null;
  }

  public void run() {
    instances = findRunningInstancesWithKeyPair(ec2, keyPairName);
    terminateInstances();
    deleteKeyPair(ec2, keyPairName);
  }

  private void terminateInstances() {
    printNoDot("Terminating Instances");

    if (instances.isEmpty())
      return;

    List<String> ips = getIps(instances);
    for (String ip : ips)
      System.out.println("    " + ip);
    TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest();
    terminateInstancesRequest.withInstanceIds(getInstanceIds(instances));

    TerminateInstancesResult result = ec2.terminateInstances(terminateInstancesRequest);
    List<InstanceStateChange> terminatingInstances = result.getTerminatingInstances();

    print("");
    printDone(getIds(terminatingInstances));
  }

  public static void main(String[] args) {
    String launchHostIp = getMyIp();
    System.out.println("Attempting to terminate all instances with keypair: " + getUniqueKeyPairName(launchHostIp));
    MultiInstanceTerminator terminator = new MultiInstanceTerminator(AmazonEC2ClientBuilder.defaultClient(),
        launchHostIp);
    terminator.run();
  }

}
