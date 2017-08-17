#!/bin/bash

if [ "$1" == "" ]; then
	echo please specify the row count as argument
	exit 1
fi

MAX=$1
echo startIP,endIP,carrier,anonymizerStatus,organization,asn,proxyType,sld,tld
COUNTER=0
STARTIP=2130702413
ENDIP=0
RECORDCOUNT=0
while [  $RECORDCOUNT -lt $MAX ]; do
 let RECORDCOUNT=RECORDCOUNT+1
 let COUNTER=COUNTER+500
 let ENDIP=STARTIP+COUNTER
 echo $STARTIP, $ENDIP,asia,mongolia,mo,91,,genghis,mail-bbc
 let STARTIP=ENDIP+1
done
