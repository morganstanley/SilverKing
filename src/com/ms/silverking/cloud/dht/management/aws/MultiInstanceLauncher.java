package com.ms.silverking.cloud.dht.management.aws;

import static com.ms.silverking.cloud.dht.management.aws.Util.debugPrint;
import static com.ms.silverking.cloud.dht.management.aws.Util.deleteKeyPair;
import static com.ms.silverking.cloud.dht.management.aws.Util.getInstanceIds;
import static com.ms.silverking.cloud.dht.management.aws.Util.getIps;
import static com.ms.silverking.cloud.dht.management.aws.Util.isRunning;
import static com.ms.silverking.cloud.dht.management.aws.Util.print;
import static com.ms.silverking.cloud.dht.management.aws.Util.printDone;
import static com.ms.silverking.cloud.dht.management.aws.Util.printInstance;
import static com.ms.silverking.cloud.dht.management.aws.Util.waitForInstancesToBeReachable;
import static com.ms.silverking.cloud.dht.management.aws.Util.waitForInstancesToBeRunning;
import static com.ms.silverking.cloud.dht.management.aws.Util.writeToFile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairResult;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.GroupIdentifier;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.KeyPair;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.UserIdGroupPair;
import com.ms.silverking.cloud.dht.management.SKCloudAdmin;

import static com.ms.silverking.cloud.dht.management.aws.Util.getMyIp;
import static com.ms.silverking.cloud.dht.management.aws.Util.getUniqueKeyPairName;
import static com.ms.silverking.cloud.dht.management.aws.Util.USER_HOME;

public class MultiInstanceLauncher {

  private final AmazonEC2 ec2;
  private final String launchHostIp;
  private final String keyPairName;
  private String amiId;
  private String instanceType;
  private final boolean includeMaster;
  private int numWorkerInstances;

  private Instance launchInstance;
  private String launchKeyPairName;
  private List<GroupIdentifier> securityGroups;
  private String subnetId;

  public static final String privateKeyFilename = USER_HOME + "/.ssh/id_rsa";
  public static final String ipsFilename = SKCloudAdmin.cloudOutDir + "/cloud_ip_list.txt";
  private String privateKey;

  private static final String newSecurityGroupName = "sk_instance";

  private List<Instance> workerInstances;

  public MultiInstanceLauncher(AmazonEC2 ec2, String launchHostIp, int numInstances, String amiId, String instanceType,
      boolean includeMaster) {
    this.ec2 = ec2;
    this.launchHostIp = launchHostIp;
    this.keyPairName = getUniqueKeyPairName(launchHostIp);
    this.amiId = amiId;
    this.instanceType = instanceType;
    this.includeMaster = includeMaster;

    // needs to be after includeMaster
    Util.checkNumInstances(numInstances);
    setNumWorkerInstances(numInstances);

    workerInstances = Collections.emptyList();
  }

  private void setNumWorkerInstances(int numInstances) {
    numWorkerInstances = numInstances;
    if (includeMaster)
      numWorkerInstances--;
  }

  public String getKeyPairName() {
    return keyPairName;
  }

  public void run() {
    System.out.println("Creating " + numWorkerInstances + " new instance(s)");

    checkIamRoleIsAttached();

    if (!isMasterOnlyInstance()) {
      setLaunchInstance();
      //        createSecurityGroup();
      //        addSecurityGroupToLaunchInstance();
    }

    deleteKeyPair(ec2, keyPairName);
    createKeyPair();
    createPrivateKeyFile();

    if (!isMasterOnlyInstance()) {
      createAndRunNewInstances();
      waitForInstancesToBeRunning(ec2, workerInstances);
      waitForInstancesToBeReachable(ec2, workerInstances);
    }

    createIpListFile();
  }

  public void checkIamRoleIsAttached() {
    DescribeInstancesRequest request = new DescribeInstancesRequest();
    try {
      DescribeInstancesResult response = ec2.describeInstances(request);
    } catch (SdkClientException exception) {
      throw new SdkClientException(
          "No AWS credentials found. You need to attach an IAM Role to this instance that has EC2 permissions.\n" +
              "You can do that from the aws console. Right click on this instance->Instance Settings->Attach/Replace " +
              "IAM Role.\n" + "If you haven't created a role, you can't attach one. If you need help creating a role," +
              " follow this guide: https://github.com/Morgan-Stanley/SilverKing/blob/master/Quick-Start-Guide" +
              ".md#CreateIAMRole");
    }
  }

  public boolean isMasterOnlyInstance() {
    return 0 == numWorkerInstances;
  }

  private void setLaunchInstance() {
    print("Setting Launch Host");

    DescribeInstancesRequest request = new DescribeInstancesRequest();
    while (true) {
      DescribeInstancesResult response = ec2.describeInstances(request);

      for (Reservation reservation : response.getReservations()) {
        for (Instance instance : reservation.getInstances()) {
          printInstance(instance);
          if (isLaunchInstance(instance)) {
            setLaunchInstance(instance);
            return;
          }
        }
      }

      if (response.getNextToken() == null)
        break;

      debugPrint("token: " + response.getNextToken());
      request.setNextToken(response.getNextToken());
    }

    throw new RuntimeException("Couldn't find launch instance");
  }

  private boolean isLaunchInstance(Instance instance) {
    if (System.getProperty("os.name").toLowerCase().startsWith("windows"))
      return isRunning(instance) && instance.getImageId().equals("ami-68790210");
    else
      return isRunning(instance) && ipMatchesThisMachine(instance);
  }

  private boolean ipMatchesThisMachine(Instance instance) {
    return instance.getPrivateIpAddress().equals(launchHostIp);
  }

  private void setLaunchInstance(Instance instance) {
    launchInstance = instance;

    if (isSet(amiId))
      amiId = "ami-" + amiId;
    else
      amiId = instance.getImageId();

    if (!isSet(instanceType))
      instanceType = instance.getInstanceType();

    launchKeyPairName = instance.getKeyName();
    securityGroups = instance.getSecurityGroups();
    subnetId = instance.getSubnetId();
    printDetails();
    printDone(instance.getInstanceId());
  }

  private boolean isSet(String value) {
    return value != null;
  }

  private void printDetails() {
    debugPrint("set launch instance: " + launchInstance);
    debugPrint("ami:    " + amiId);
    debugPrint("type:   " + instanceType);
    debugPrint("kp:     " + launchKeyPairName);
    debugPrint("sg:     " + securityGroups);
    debugPrint("subnet: " + subnetId);
  }

  private void createSecurityGroup() {
    //        DescribeSecurityGroupsResult dsgResult = ec2.describeSecurityGroups();
    //        debugPrint(dsgResult);

    CreateSecurityGroupRequest sgRequest = new CreateSecurityGroupRequest();
    sgRequest.withGroupName(newSecurityGroupName).withDescription("For running sk instance(s)");
    CreateSecurityGroupResult createSecurityGroupResult = ec2.createSecurityGroup(sgRequest);

    IpPermission ipPermission = new IpPermission();
    UserIdGroupPair pair = new UserIdGroupPair();
    pair.withGroupName(
        newSecurityGroupName)    // or could have used .withGroupId(createSecurityGroupResult.getGroupId()) instead
        .withDescription("so machines can talk to each other");
    ipPermission.withUserIdGroupPairs(Arrays.asList(pair)).withIpProtocol("-1").withFromPort(-1).withToPort(-1);
    AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest =
        new AuthorizeSecurityGroupIngressRequest();
    authorizeSecurityGroupIngressRequest.withGroupName(newSecurityGroupName).withIpPermissions(ipPermission);

    ec2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);
  }

  // we have to create a new keypair b/c the keypair we used to get access (ssh) to this master/launch instance, that
  // key_pair is on our windows machine.. i.e. outside aws
  // this applies for masterOnlyInstances as well. w/o this, we can't even ssh to ourselves with masterOnlyInstances
  // b/c of "Permission denied (publickey)"
  private void createKeyPair() {
    print("Creating New Key Pair");

    CreateKeyPairRequest createKeyPairRequest = new CreateKeyPairRequest();
    createKeyPairRequest.withKeyName(keyPairName);

    CreateKeyPairResult createKeyPairResult = ec2.createKeyPair(createKeyPairRequest);

    KeyPair keyPair = createKeyPairResult.getKeyPair();
    privateKey = keyPair.getKeyMaterial();

    printDone(keyPairName);
  }

  private void createPrivateKeyFile() {
    print("Creating New Private Key File");

    writeToFile(privateKeyFilename, privateKey);

    printDone(privateKeyFilename);
  }

  private void createAndRunNewInstances() {
    print("Creating New Instances");

    RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
    runInstancesRequest.withImageId(amiId).withInstanceType(instanceType).withMinCount(1).withMaxCount(
        numWorkerInstances).withKeyName(keyPairName).withSecurityGroupIds(
        getSecurityGroupIds(securityGroups))    // for some reason this one works and below doesn't
        //                           .withSecurityGroups( getNames(securityGroups) )    if you try to use
        //                           .withSecurityGroups AND .withSubnetId, you will get Exception in thread "main"
        //                           com.amazonaws.services.ec2.model.AmazonEC2Exception: The parameter groupName
        //                           cannot be used with the parameter subnet (Service: AmazonEC2; Status Code: 400;
        //                           Error Code: InvalidParameterCombination; Request ID:
        //                           a230cc97-c84b-4253-bdf0-874c68759efd)
        .withSubnetId(subnetId);

    RunInstancesResult result = ec2.runInstances(runInstancesRequest);
    workerInstances = result.getReservation().getInstances();

    printDone(getInstanceIds(workerInstances));
  }

  private List<String> getSecurityGroupIds(List<GroupIdentifier> securityGroups) {
    List<String> names = new ArrayList<>();

    for (GroupIdentifier group : securityGroups)
      names.add(group.getGroupId());

    return names;
  }

  //    private List<String> getNames(List<GroupIdentifier> securityGroups) {
  //        List<String> names = new ArrayList<>();
  //
  //        for (GroupIdentifier group : securityGroups)
  //            names.add(group.getGroupName());
  //
  //        return names;
  //    }

  private void createIpListFile() {
    print("Creating IpList File");

    writeToFile(ipsFilename, getInstanceIps());

    printDone(ipsFilename);
  }

  public List<String> getInstanceIps() {
    if (isMasterOnlyInstance())
      return Arrays.asList(launchHostIp);
    else {
      List<String> instanceIps = getWorkerIps();
      if (includeMaster)
        instanceIps.add(0, launchHostIp);

      return instanceIps;
    }
  }

  public List<String> getWorkerIps() {
    return getIps(workerInstances);
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0)
      throw new RuntimeException("We need to know how many instances to start. Please pass in <numberOfInstances>");

    int numInstances = Integer.valueOf(args[0]);
    System.out.println(
        "Attempting to launch " + (numInstances - 1) + " new instances, for a total of " + numInstances + " (this " +
            "instance + those " + (numInstances - 1) + ")");
    MultiInstanceLauncher launcher = new MultiInstanceLauncher(AmazonEC2ClientBuilder.defaultClient(), getMyIp(),
        numInstances, null, null, true);
    launcher.run();
  }

}
