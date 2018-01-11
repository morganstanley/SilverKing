#!/bin/ksh

source lib/common.lib

# params
typeset release_area=$1

if [[ -z $release_area ]] ; then
	release_area="../release-x.xx"
	echo "Set release_area=$release_area"
	echo
fi
	
f_printHeader "RELEASING"

f_makeVerbose "$release_area"
f_copy "$BIN_DIR"     "$release_area"
f_copy "$DOC_DIR"     "$release_area"
f_copy "$LIB_DIR"     "$release_area"
f_copy "$INCLUDE_DIR" "$release_area"
