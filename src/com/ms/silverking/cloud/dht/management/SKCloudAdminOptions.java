package com.ms.silverking.cloud.dht.management;

import org.kohsuke.args4j.Option;

class SKCloudAdminOptions {

  static final int defaultNumInstances = -1;

  SKCloudAdminOptions() {
  }

  @Option(name = "-c", aliases = {
      "--command" }, usage = "command. eg: \"LaunchInstances\", \"StartInstances\", \"StopInstances\", " +
      "\"TerminateInstances\", \"StartSpark\", \"StopSpark\"", required = true)
  String command;

  @Option(name = "-n", aliases = {
      "--num-of-instances" }, usage = "numberOfInstances. eg: \"1\", \"50\", \"1000\", etc.", required = false)
  int numInstances = defaultNumInstances;

  @Option(name = "-a", aliases = {
      "--ami-id" }, usage = "amiId. eg: \"68790210\", \"bfe4b5c7\", etc.", required = false)
  String amiId;

  @Option(name = "-i", aliases = {
      "--instance-type" }, usage = "instanceType. eg: \"t2.micro\", \"m5d.large\", \"i3.metal\", etc.", required =
      false)
  String instanceType;

  @Option(name = "-e", aliases = {
      "--exclude-master" }, usage = "excludeMaster from being one of the \"numberOfInstances\"", required = false)
  boolean excludeMaster;

  @Option(name = "-r", aliases = {
      "--replication" }, usage = "replication factor for the SilverKing cluster", required = false)
  int replication = 1;

  @Option(name = "-d", aliases = { "--data-home" }, usage = "data base home for the SilverKing data", required = false)
  String dataBaseVar;

}
