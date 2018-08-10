#!/bin/ksh

source `dirname $0`/../lib/run_scripts_from_any_path.snippet

cd ..
source lib/common.lib
cd -

rm ~/.ssh/id_rsa*
rm -rf /tmp
rm -rf /var/tmp
rm $TESTING_DIR/${SK_GRID_CONFIG_NAME}.env
rm $AWS_DIR/zookeeper.out
# rm -rf $TESTING_DIR/$GTEST_FOLDER_NAME
# rm -rf $TESTING_DIR/$JUNIT_FOLDER_NAME

# This will support at least the t2.micro and i3.large instance types, mounting the nvme drive for the i3.large type
echo "" >> /etc/rc.local
echo "
if [ -b /dev/nvme0n1 ]; then
    mkfs.ext4 -E nodiscard /dev/nvme0n1
    mkdir /mnt/silverking
    mount -o discard /dev/nvme0n1 /mnt/silverking
    chown ec2-user:ex2-user /mnt/silverking
fi
" >> /etc/rc.local
