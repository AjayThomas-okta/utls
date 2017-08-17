#!/bin/bash

if [ "$1" == "" ]; then
	echo please specify the row count as argument
	exit 1
fi

MAX=$1
echo startIP,endIP,carrier,anonymizerStatus,organization,asn,proxyType,sld,tld
COUNTER=0
STARTIP=1
ENDIP=0
while [  $COUNTER -lt $MAX ]; do
 let COUNTER=COUNTER+1
 let ENDIP=STARTIP+COUNTER
 echo $STARTIP, $ENDIP,asia,mongolia,mo,91,,genghis,mail-bbc
 let STARTIP=ENDIP+1
done
