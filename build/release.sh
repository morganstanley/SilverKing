#!/bin/ksh

source lib/common.lib

# params
RELEASE_AREA=$1

if [[ -z $RELEASE_AREA ]] ; then
	RELEASE_AREA="../release-x.xx"
	echo "Set RELEASE_AREA=$RELEASE_AREA"
	echo
fi
	
f_printHeader "RELEASING"

f_makeVerbose "$RELEASE_AREA"
f_copy "$BIN_DIR"     "$RELEASE_AREA"
f_copy "$DOC_DIR"     "$RELEASE_AREA"
f_copy "$LIB_DIR"     "$RELEASE_AREA"
f_copy "$INCLUDE_DIR" "$RELEASE_AREA"
