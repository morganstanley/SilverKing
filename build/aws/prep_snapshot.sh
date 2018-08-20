#!/bin/ksh

source `dirname $0`/../lib/run_scripts_from_any_path.snippet

cd ..
source lib/common.lib
cd -

rm ~/.ssh/id_rsa*
rm -rf /tmp/*
rm -rf /var/tmp/*
rm $TESTING_DIR/${SK_GRID_CONFIG_NAME}.env
rm $AWS_DIR/zookeeper.out
# rm -rf $TESTING_DIR/$GTEST_FOLDER_NAME
# rm -rf $TESTING_DIR/$JUNIT_FOLDER_NAME

# This will support at least the t2.micro and i3.large instance types, mounting the nvme drive for the i3.large type
mnt=/mnt/silverking
nvme=/dev/nvme0n1
# can't do this, it won't append, even adding sudo to echo or before /etc/rc.local won't do the trick: https://superuser.com/questions/136646/how-to-append-to-a-file-as-sudo
# echo "" >> /etc/rc.local
# I like this one:
#   sudo bash -c "somecommand >> somefile"
# over:
#   echo "output" | sudo tee -a file
# b/c tee will output the "output" to the shell as well, and I want no output. I only want output if there is an error.
sudo bash -c 'echo "

if [ -b $nvme ]; then
    mkfs.ext4 -E nodiscard $nvme
    mkdir $mnt
    mount -o discard $nvme $mnt
    chown ec2-user:ex2-user $mnt
fi
" >> /etc/rc.local'
