package com.ms.silverking.cloud.dht.management.aws;

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
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesResult;

import static com.ms.silverking.cloud.dht.management.aws.Util.getMyIp;
import static com.ms.silverking.cloud.dht.management.aws.Util.getUniqueKeyPairName;

public class MultiInstanceStopper {

  private final AmazonEC2 ec2;
  private final String keyPairName;
  private List<Instance> instances;

  public MultiInstanceStopper(AmazonEC2 ec2, String launchHostIp) {
    this.ec2 = ec2;
    this.keyPairName = getUniqueKeyPairName(launchHostIp);

    instances = null;
  }

  public void run() {
    instances = findRunningInstancesWithKeyPair(ec2, keyPairName);
    stopInstances();
  }

  private void stopInstances() {
    printNoDot("Stopping Instances");

    if (instances.isEmpty())
      return;

    List<String> ips = getIps(instances);
    for (String ip : ips)
      System.out.println("    " + ip);
    StopInstancesRequest stopInstancesRequest = new StopInstancesRequest();
    stopInstancesRequest.withInstanceIds(getInstanceIds(instances));

    StopInstancesResult result = ec2.stopInstances(stopInstancesRequest);
    List<InstanceStateChange> stoppingInstances = result.getStoppingInstances();

    print("");
    printDone(getIds(stoppingInstances));
  }

  public static void main(String[] args) {
    String launchHostIp = getMyIp();
    System.out.println("Attempting to stop all instances with keypair: " + getUniqueKeyPairName(launchHostIp));
    MultiInstanceStopper stopper = new MultiInstanceStopper(AmazonEC2ClientBuilder.defaultClient(), launchHostIp);
    stopper.run();
  }

}
