# How to Guide
## Try a Demo
If you'd like to just give SilverKing a try, you can create an AWS instance from our template:
    Region: US West (Oregon)
    AMI-Name: silverking
    https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#Images:visibility=public-images;search=silverking;sort=name
### Run
[GitHub](http://github.com)<br>
[google](http://www.google.com/) inline link.

    [This link](http://example.com/ "Title") has a title attribute.

    Links are also auto-detected in text: http://example.com/
#### Single-instance
    cd ~/SilverKing/build/aws/
    ./aws_start.sh
    cd ~/SilverKing/bin
    ./skc -G ../build/testing -g GC_sk_test

#### Multi-instance
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


## Build
### Amazon-linux
```bash
sudo yum -y install git ksh
git clone https://github.com/Morgan-Stanley/SilverKing.git
cd ~/SilverKing/build/aws
./aws_build.sh
```

### Ubuntu
```ksh
sudo apt-get update
sudo apt-get install git ksh
git clone https://github.com/Morgan-Stanley/SilverKing.git
cd ~/SilverKing/build/aws
./ubuntu_build.sh
```