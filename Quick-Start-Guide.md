# Quick-start Guide
## Running Simple AWS-based Instances
If you'd like to just give SilverKing a try, you can be up and running in minutes using an AWS instance from our template:<br>
&emsp;Region: US West (Oregon)<br>
&emsp;AMI-Name: silverking<br>
&emsp;[Silverking AMI](https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#Images:visibility=public-images;search=silverking;sort=name)<br>
### Running on AWS
#### Single-instance Cluster

Once you have an AWS SilverKing AMI instance running, first sanity check to ensure that local hostname resolution is working:
```ksh
hostname -i
10.0.0.1  # an ip should be returned
```
If hostname -i fails to return an IP address, you must either modify your instance to correct this, or add an entry in /etc/hosts.

With that sanity check out of the way, you can start up SilverKing as follows:
```ksh
cd ~/SilverKing/build/aws/
./aws_start.sh  # this starts zookeeper, sk, and skfs
```
That's it! SilverKing is up and running.

Once that completes you can perform key-value operations using the skc tool:
```ksh
~/SilverKing/bin/skc -G ~/Silverking/build/testing -g GC_SK_test    # this is a silverking client, you can test puts and gets. type "skc> h;" to see the help menu.
```

You can also use the SKFS file system:
```ksh
cd /var/tmp/silverking/skfs/skfs_mnt/skfs
echo World > Hello
cat Hello
```

#### Multi-instance Cluster
Running a multi-instance cluster presently requires some work to ensure that the instances can work together. (This will be simplified in the future.)
    - Start N instance from image
    - For each instance N
        cd ~/SilverKing/build/aws && ./aws_multi_genKey.sh
    - Machine1 (repeat for every machine)
        cd ~/.ssh
            vi authorized keys
                □ add machine2..N key id_rsa.pub
    - Machine1
        cd ~/SilverKing/build/aws
        vi multi_nonlaunch_machines_list.txt
            Add machines 2..N (make sure there is an empty line at the end!)
        ./aws_multi_start.sh
        ./aws_multi_stop.sh


## Building SilverKing on AWS
Our AMI instance includes simplified build scripts for Amazon-Linux and Ubuntu.

### Amazon-Linux
```
sudo yum -y install git ksh
cd ~
git clone https://github.com/Morgan-Stanley/SilverKing.git
cd ~/SilverKing/build/aws
./aws_build.sh
```

### Ubuntu
```
sudo apt-get update
sudo apt-get install git ksh
cd ~
git clone https://github.com/Morgan-Stanley/SilverKing.git
cd ~/SilverKing/build/aws
./ubuntu_build.sh
```