package com.ms.silverking.cloud.dht.management.aws;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;

public class ExampleTestCode {

  private AmazonEC2 ec2;
  private static final String amiId = "ami-b77b06cf";
  private static final String keyPairName = "bph";
  private static final String securityGroupName = "bph";

  public ExampleTestCode() {
    /*
     * The ProfileCredentialsProvider will return your [default]
     * credential profile by reading from the credentials file located at
     * (C:\\Users\\ben-pc\\.aws\\credentials).
     */
    ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
    try {
      credentialsProvider.getCredentials();
    } catch (Exception e) {
      throw new AmazonClientException(
          "Cannot load the credentials from the credential profiles file. " + "Please make sure that your credentials" +
              " file is at the correct " + "location (C:\\Users\\ben-pc\\.aws\\credentials), and is in valid format.",
          e);
    }
    ec2 = AmazonEC2ClientBuilder.standard().withRegion("us-west-2").build();
  }

  public void run() {
    try {
      DescribeAvailabilityZonesResult availabilityZonesResult = ec2.describeAvailabilityZones();
      System.out.println(
          "You have access to " + availabilityZonesResult.getAvailabilityZones().size() + " Availability Zones.");

      DescribeInstancesResult describeInstancesRequest = ec2.describeInstances();
      List<Reservation> reservations = describeInstancesRequest.getReservations();
      Set<Instance> instances = new HashSet<Instance>();

      System.out.println("Reserves size: " + reservations.size());
      for (Reservation reservation : reservations) {
        instances.addAll(reservation.getInstances());
      }

      System.out.println("You have " + instances.size() + " Amazon EC2 instance(s) running.");
    } catch (AmazonServiceException ase) {
      System.out.println("Caught Exception: " + ase.getMessage());
      System.out.println("Reponse Status Code: " + ase.getStatusCode());
      System.out.println("Error Code: " + ase.getErrorCode());
      System.out.println("Request ID: " + ase.getRequestId());
    }
  }

  public void runInstances() {
    RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
    runInstancesRequest.withImageId(amiId).withInstanceType("t2.micro").withMinCount(1).withMaxCount(1).withKeyName(
        keyPairName).withSecurityGroups(securityGroupName);

    RunInstancesResult result = ec2.runInstances(runInstancesRequest);
  }

  public void test() {
    final AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();
    boolean done = false;

    DescribeInstancesRequest request = new DescribeInstancesRequest();
    while (!done) {
      DescribeInstancesResult response = ec2.describeInstances(request);

      for (Reservation reservation : response.getReservations()) {
        for (Instance instance : reservation.getInstances()) {
          System.out.printf(
              "Found instance with id %s, " + "AMI %s, " + "type %s, " + "state %s " + "and monitoring state %s%n",
              instance.getInstanceId(), instance.getImageId(), instance.getInstanceType(),
              instance.getState().getName(), instance.getMonitoring().getState());
        }
      }

      request.setNextToken(response.getNextToken());

      if (response.getNextToken() == null) {
        done = true;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    System.out.println("===========================================");
    System.out.println("Welcome to the AWS Java SDK!");
    System.out.println("===========================================");

    ExampleTestCode tester = new ExampleTestCode();
    tester.run();
    tester.test();
    //        tester.runInstances();
  }

}
