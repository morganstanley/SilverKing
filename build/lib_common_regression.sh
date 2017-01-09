#!/bin/ksh

source lib_common.sh

function f_checkAndCdToRegressionArea {
	if [[ -z $REGRESSION_AREA ]]; then
		echo "Must pass in a path for the regression area, so we can cd to this area, make a folder, and kick off a run"
		exit
	fi

	cd $REGRESSION_AREA 
}

function f_makeSetAndChangeToFolder {
    FOLDER_NAME=$(f_getBuildTime)
	mkdir $FOLDER_NAME
	cd $FOLDER_NAME
}

# sendmail - good, can send html body, but attachment is pain
#     mail - good, but only one line in body and no html, attachment is easy tho
function f_sendEmail {
	typeset outputFile=$1
	typeset to=$2
	
	typeset   passCount=$(f_getCount "$outputFile" "PASS")
	typeset   failCount=$(f_getCount "$outputFile" "FAIL")
	typeset      result=$(f_getResult "$failCount" "0")
	typeset summaryText=$(f_extractSummaryFromFile "$outputFile")
	
	typeset    from="regression@silverking.com"
	typeset subject="Regression $FOLDER_NAME $result - (p=$passCount, f=$failCount)"
	typeset summaryTrimmed=`echo "$summaryText" | tr -d '\n'`
	
	echo "$summaryText" | $MUTT -e "my_hdr From:$from" $to -s "$subject" -a "$outputFile"  
	
	#echo "$summaryText" > abc.txt
	#tr '\n' '+' < abc.txt > abc2.txt
	#mail -s "Regression $FOLDER_NAME $result - (p=$passCount, f=$failCount)" -a $outputFile user@domain.com < abc2.txt
	
	#(
		#echo "From: $from"
		#echo "To: $to"
		#echo "Subject: $subject"
		##echo "Content-Type: text/html"
		#echo
		#echo "$summaryTrimmed"
		#echo
	#) | sendmail -t
	#echo -e "To: $to\nSubject: $subject\n\n$summaryText" | sendmail -t	# important to have double \n between headers and body
}

function f_extractSummaryFromFile {
	typeset outputFile=$1
	typeset   startLine=`sed -n '/+++/=' $outputFile`
	typeset     endLine=`wc -l < $outputFile`
	typeset summaryText=`sed -n "${startLine},${endLine}p" $outputFile`
	echo "$summaryText"	# double quotes very important, preserves the multi-line result
}
