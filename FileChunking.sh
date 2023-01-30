#!/bin/sh
####################################################################################
# Name: 
# 
####################################################################################
# This script performs the following functions:
# 1. 
# 2. 
#
####################################################################################
# All scritps should include ETLLib.sh to set path and environment variables
# This will get the absolute path from where the script is executed from

SPLIT_ROWS_COUNT=${1};
INPUT_FILE_NAME_TO_SPLIT=${2};
OUTPUT_FILE_NAME_PATTERN=${3};
#SUBJECT_AREA=${4}
CURR_DIR=`pwd`;
LIST_FILE=split_files.txt;
CURR_TMSTP=`echo $(date +%Y-%m-%d" "%H:%M:%S)| sed -e 's/-//g' -e 's/://g' -e 's/ //g' | cut -c1-14`;

__X=`dirname "$(cd "${0%/*}" 2>/dev/null; echo "$PWD"/"${0##*/}")"`
cd $__X
SUBJECT_AREA=$__X

# Now walk down the directory structure until we find the "scriptfiles" directory.
# It is assumed that the script is executed from a directory under the "scriptfiles" dirctory.
__found=1
while [ "${__found}" -ne 0 ]
do
    CURRENTNODE=`pwd | awk '{n=0; split($1,ndirs,"/"); for (i in ndirs) n++; print ndirs[n];}'`
    if [ "${CURRENTNODE}" == "" ]
    then
        exit 1
    else
        if [ "${CURRENTNODE}" == "scriptfiles" ] 
        then
            SCRIPTFILESDIR=`pwd`
            __found=0
        else
            cd ..
        fi
    fi
done 

. ${SCRIPTFILESDIR}/ETLLib.sh          # Source ETLIb.sh

#===================================================================================
exitScript()  # Cleanup and Exit the script and return to the calling program.
#===================================================================================
#Arg_1  The exit code to return back
{
    unsetEnvironmentVariables
    exit ${1}
}
. ${SCRIPTFILESDIR}/ETLLib.sh          # Source ETLIb.sh

# Main function that sets up all the activities for dsJobRun and dsJobStatus
setEnvironmentVariables $SUBJECT_AREA  

#===================================================================================
chunkingLogic()  # Chunks the Big file into smaller ones.
#===================================================================================
{
RS_SPLIT="-1"
echo "split -l ${SPLIT_ROWS_COUNT}  ${CHUNK_INPUT} ${CHUNK_OUTPUT}"
split -l ${SPLIT_ROWS_COUNT}  ${CHUNK_INPUT} ${CHUNK_OUTPUT};
RS_SPLIT=`echo $?`
}
#===================================================================================
writeLogFile()  # Log the process information
#===================================================================================
{
writeLogFile_Input=${1};
echo ${writeLogFile_Input} >> ${LOG_DIR}/${SCRIPTNAME}_chunking.log
}
#*********************************************************************#
#* Mainline of the script                                            *#
#*********************************************************************#
CHUNKED_LIST="${IF_DIR}/${SUBJECT_AREA}/${LIST_FILE}";
rm -f ${CHUNKED_LIST}
CHUNK_INPUT="${IF_DIR}/${SUBJECT_AREA}/${INPUT_FILE_NAME_TO_SPLIT}"
CHUNK_OUTPUT="${IF_DIR}/${SUBJECT_AREA}/${OUTPUT_FILE_NAME_PATTERN}"

echo "---"
echo "$CHUNK_INPUT"
echo "---"
echo "$CHUNK_OUTPUT"

if ([ "x$SPLIT_ROWS_COUNT" == "x" ] \
	|| [ "x$INPUT_FILE_NAME_TO_SPLIT" == "x" ] \
	|| [ "x$OUTPUT_FILE_NAME_PATTERN" == "x" ]);then
	echo "PASS INPUTS"
	exit 1;
fi

chunkingLogic;

ls -1 ${IF_DIR}/${SUBJECT_AREA} | grep "${OUTPUT_FILE_NAME_PATTERN}" > ${CHUNKED_LIST}

if [ ${RS_SPLIT} == "0" ];then
	TOTAL_FILES_SPLIT=`wc -l ${CHUNKED_LIST}| cut -d ' ' -f 1`
	LINE_INDICATOR="1"
	IFS='';
		while read line
		do
			FILE_2_CHANGE=$line;
			NEW_NAME_TEMP=`echo ${INPUT_FILE_NAME_TO_SPLIT} | cut  -d '.' -f 1`
			NEW_NAME=${NEW_NAME_TEMP}_${LINE_INDICATOR}.txt;
			mv  ${IF_DIR}/${SUBJECT_AREA}/${FILE_2_CHANGE} ${IF_DIR}/${SUBJECT_AREA}/${NEW_NAME}
			#writeWarningFile " ....... Split File (${LINE_INDICATOR}) of ${TOTAL_FILES_SPLIT} : ${NEW_NAME} "
			writeLogFile " ....... Split File (${LINE_INDICATOR}) of ${TOTAL_FILES_SPLIT} : ${NEW_NAME} "
		   ((LINE_INDICATOR+=1));
		done < ${CHUNKED_LIST}

	writeLogFile "---------------------------------------------------------------------------------------------------------------"
	writeLogFile " Splitting files was SUCCESSFUL when executing the below command" 
	writeLogFile " COMMAND RUN ==> split -l ${SPLIT_ROWS_COUNT} ${CHUNK_INPUT} ${CHUNK_OUTPUT}"
	writeLogFile "---------------------------------------------------------------------------------------------------------------"
	exitScript 0
else
	writeWarningFile "---------------------------------------------------------------------------------------------------------------"
	writeWarningFile " Splitting files  FAILED when executing the below command" 
	writeWarningFile " 	COMMAND RUN ==> split -l ${SPLIT_ROWS_COUNT} ${CHUNK_INPUT} ${CHUNK_OUTPUT}"
	writeWarningFile "---------------------------------------------------------------------------------------------------------------"
	exitScript 1
fi
