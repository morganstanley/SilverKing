#!/bin/ksh

cd ..
source lib/common.lib
cd -

rm ~/.ssh/id_rsa*
rm -rf /tmp
rm -rf /var/tmp
rm $TESTING_OUTPUT_DIR/GC_sk_test.env
rm $BUILD_DIR/zookeeper.out
# rm -rf $TESTING_OUTPUT_DIR/$GTEST_FOLDER_NAME
# rm -rf $TESTING_OUTPUT_DIR/$JUNIT_FOLDER_NAME