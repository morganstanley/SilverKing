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