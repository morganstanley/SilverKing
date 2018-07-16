# Quick-Start Guide
## Running Simple AWS-based Instances
If you'd like to give SilverKing a try, you can be up and running in minutes using an AWS instance from our template:<br>
&emsp;[SilverKing AMI](https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#Images:visibility=public-images;search=SilverKing;sort=name) (ctrl+click to open in new tab)<br>
&emsp;AMI-Name: SilverKing<br>
&emsp;Platform: Amazon-Linux<br>
&emsp;Username: ec2-user<br>
&emsp;Region: US West (Oregon)<br>

*Make sure your security group contains an **Inbound Rule** for **All Traffic** with your _security group ID_ as the **Source**:
![Security Group Inbound Rule](img/sg_inbound_rule.png)<br>
*Make sure whatever VPC you are using with your instance, dns **resolution** and **hostnames** are both set to 'yes':
![VPC DNS](img/vpc_dns.PNG)

### Running on AWS
#### Single-instance Cluster

Once you have an AWS SilverKing AMI instance running, you can start up SilverKing as follows:
```ksh
~/SilverKing/build/aws/start.sh  # this starts zookeeper, sk, and skfs
```
That's it! SilverKing is up and running. You can run 'ps uxww' to see all three processes.

You can now perform key-value operations using the skc tool ([skc usage](https://morgan-stanley.github.io/SilverKing/doc/Shell.html)):
```ksh
~/SilverKing/bin/skc -G ~/SilverKing/build/testing -g GC_SK_test    
skc> h;         # help menu
skc> cn testNs; # this creates a namespace
skc> p k1 v1;   # this puts a key: k1, with value: v1
skc> g k1 v1;   # gets the values for keys: k1 and v1
        k1 => v1
        v1 => <No such value>
skc> q;         # quit
```

You can also use the SKFS file system:
```ksh
cd /var/tmp/silverking/skfs/skfs_mnt/skfs
echo World > Hello
cat Hello
```

To shut everything down:
```ksh
~/SilverKing/build/aws/stop.sh  # this stops zookeeper, sk, and skfs
```

#### Multi-instance Cluster
Running a multi-instance cluster presently requires some work to ensure that the instances can work together. (This will be simplified in the future.)

    - Start N instance from image
    - For each instance N
        ~/SilverKing/build/aws/multi_genKey.sh
    - Machine1 (repeat for every machine)
        cd ~/.ssh
            vi authorized keys
                □ add machine2..N key id_rsa.pub
    - Machine1
        cd ~/SilverKing/build/aws
        vi multi_nonlaunch_machines_list.txt
            Add machines 2..N (make sure there is an empty line at the end!)
        ./multi_start.sh
        ./multi_stop.sh


## Building SilverKing on AWS
You may also build SilverKing on AWS using simplified build scripts for both Amazon-Linux and Ubuntu.

### Amazon-Linux
```ksh
sudo yum -y install git ksh
cd ~
git clone https://github.com/Morgan-Stanley/SilverKing.git
~/SilverKing/build/aws/build_linux.sh
```

### Ubuntu
```ksh
sudo apt-get update 
sudo apt-get install git ksh    # 'apt-get update' first, or else you will get: "E: Unable to locate package ksh"
cd ~
git clone https://github.com/Morgan-Stanley/SilverKing.git
~/SilverKing/build/aws/build_ubuntu.sh
```