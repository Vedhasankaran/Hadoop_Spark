#!/bin/sh
#----------------------------------------------------------------------------------------#
#This script deploys the project components into the NFS and HDFS                        #
#Workflows & jars are copied to HDFS and Falcons are copied to NFS                       #
#                                                                                        #
#Author : HVEDHASA                                                                       #
#PARAMETERS PASSED  :                                                                    #
#                    1. ENV : Eg:. dev,qa,prod                                           #
#                    2. SRC_MODULE : Eg:. 10002_sls,10003_cpo,etc.                       #
#                    3. DEPLOYED FILE LOCATION IN UNIX  : Eg: /s/iapxhtam/POLK/return/   #
#                    4. PURGE DEPLOYED SOURCE  FILE : Eg: Y/y -> Yes ; any other -> No   #
#----------------------------------------------------------------------------------------#
echo -e "\033c\e[3J";
#---------------------------------------------------------------
#CONSTANTS Declaration
#---------------------------------------------------------------
COS_FILLER_SPACES=" ";
COS_FILLER_85_EQUALS="====================================================================================";
COS_FILLER_85_MINUS="------------------------------------------------------------------------------------";
export COLR_BRT_YELLOW='\e[1;33m'
export COLR_BRT_GREEN='\033[1;32m';
export COLR_END='\e[0m' ;
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} $COS_FILLER_85_EQUALS ${COLR_END};
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "  *  *  *       WELCOME   TO   SCA-C  DEPLOYMENT  PROCESS       *  *  * "${COLR_END};
	echo $COS_FILLER_SPACES ;
	echo -e ${COLR_BRT_GREEN} $COS_FILLER_85_EQUALS ${COLR_END};
	echo $COS_FILLER_SPACES;

echo -en ${COLR_BRT_YELLOW} "ENTER THE ENVIRONMENT (dev/qa/edu/prod)            : "${COLR_END} 
read ENV1
ENV=`echo $ENV1 | tr '[:upper:]' '[:lower:]'`
if [ "x$ENV" == "x" ];then
   echo "Please enter valid Environement (dev/qa/edu/prod) !"
   exit 1
fi

echo ""

echo -en ${COLR_BRT_YELLOW} "ENTER SOURCE NAME WITH CCR (eg: 10002_sls)         : "${COLR_END}
read SRC_MODULE1
SRC_MODULE=`echo $SRC_MODULE1 | tr '[:upper:]' '[:lower:]'`
if [ "x$SRC_MODULE" == "x" ];then
   echo "Please enter valid Source Name with CCR (eg: 10002_sls)  !"
   exit 1
fi

echo ""

echo -en ${COLR_BRT_YELLOW} "ENTER THE UNIX LOCATION OF 'Deployment.zip' 
(eg--> /s/iapxhtam/POLK/vin/ )                      : "${COLR_END} 
read DEPLYD_LOCATION_UNIX
if [ "x$DEPLYD_LOCATION_UNIX" == "x" ];then
   echo "Please enter valid Unix Location of the 'Deployment.zip' file (eg: /s/iapxhtam/POLK/vin/) !"
   exit 1
else
	if [ -d "$DEPLYD_LOCATION_UNIX/" ];then
		echo "";
	else
		echo -e ${COLR_BRT_YELLOW} " !!!! ERROR !!! ERRROR  "${COLR_END}
		echo -e ${COLR_BRT_YELLOW} " !!!! ERROR !!! ERRROR - Folder '$DEPLYD_LOCATION_UNIX/' doesn't not exist "${COLR_END}
		echo -e ${COLR_BRT_YELLOW} " !!!! ERROR !!! ERRROR  "${COLR_END}
		exit 1;
	fi
fi

echo ""

echo -en ${COLR_BRT_YELLOW} "DELETE 'deployment.zip' UPON SUCCESSFUL DEPLOYMENT ? 
(Y / y -> yes  ; any ther Key -> No)                : " ${COLR_END}
read DELETE_DEPLOYMENT_ZIP_FILE1
DELETE_DEPLOYMENT_ZIP_FILE=`echo $DELETE_DEPLOYMENT_ZIP_FILE1 | tr '[:lower:]' '[:upper:]'`
if [ "x$DELETE_DEPLOYMENT_ZIP_FILE" == "" ];then
   echo "Please enter valid Option (Y / y -> yes  ; any ther Key -> No)!"
   exit 1
fi

#--------------------
#Variable Declaration
#
#--------------------
PROGNAME=$(basename $0)
CURR_DIR=`pwd`;
CURR_USER=`whoami`;
CURR_DIR=${DEPLYD_LOCATION_UNIX};
CURR_SERVER=`hostname -s`;
CURR_TMSTP=`echo $(date +%Y-%m-%d" "%H:%M:%S)| sed -e 's/-//g' -e 's/://g' -e 's/ //g' | cut -c1-14`

#ENV=$1;
#SRC_MODULE=$2;
#----------------------------------------------------------------
#       Function for exit due to fatal program error
#----------------------------------------------------------------
function error_exit
{
	echo -e ${COLR_BRT_GREEN} "*----------------------------------------------------------------------*"${COLR_END}
	echo -e ${COLR_BRT_GREEN} "*         ERROR * ERROR * ERROR * ERROR * ERROR * ERROR * ERROR * ERROR    *"${COLR_END}
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "ERROR REASON : $1"${COLR_END}
	echo -e ${COLR_BRT_GREEN} "               $2"${COLR_END}
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "*            Please Contact the IT Team for details                    *"${COLR_END}
	echo -e ${COLR_BRT_GREEN} "*----------------------------------------------------------------------*"${COLR_END}
	exit
}
#-----------------------------------------------------------------------
#       Function to continue processing due to success of previous step
#-----------------------------------------------------------------------
function success_continue
{
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "*Success   *Success - ${1} "${COLR_END}
	echo $COS_FILLER_SPACES;
}
#----------------------------------------------------------------
#       Function for enabling Kerberos
#----------------------------------------------------------------
function perform_0001_enabling_kerberos
{
echo "Enabling Kerberos...."

	kinit -k -t /s/$CURR_USER/$CURR_USER.keytab $CURR_USER; RS_KINIT=`echo $?`;
if  [ $RS_KINIT -eq 0 ]
	then
	success_continue "Kinit is SUCCESSFUL";
	hadoop fs -ls /project/scac/$ENV/sourcecode/$SRC_MODULE/ ; RS_HDFS_TARGET=`echo $?`;
		if [ $RS_HDFS_TARGET -eq 0 ]
		then
			success_continue "HDFS Path /project/scac/$ENV/sourcecode/$SRC_MODULE/ is Truly Avaialable";
		else
			error_exit "HDFS Path /project/scac/$ENV/sourcecode/$SRC_MODULE/ is NOT Avaialable";
		fi
else
		error_exit  " FAILED to Execute KINIT file $CURR_USER.keytab in the Location: /s/$CURR_USER/ "
fi
}
#----------------------------------------------------------------
#  PERFORM UNZIP in NFS
#----------------------------------------------------------------
function perform_0002_unzip_deployment
{
	unzip  -o $CURR_DIR/deployment; RS_UNZIP=`echo $?`;
	if  [ $RS_UNZIP -eq 0 ]
		then
		success_continue "Unzip of $CURR_DIR/deployment.zip SUCCESS"
	else
		error_exit "Unzip of $CURR_DIR/deployment.zip Failed"
	fi
}
#----------------------------------------------------------------
#  PERFORM DOS2UNIX FOR NFS SCRIPTS
#----------------------------------------------------------------
function perform_0003_dos2unix_nfs
{
	dos2unix $CURR_DIR/deployment/nfs/initial-setup/*.sh; RS_D2U_NFS=`echo $?`;
	if  [ $RS_D2U_NFS -eq 0 ]
		then
		success_continue "DOS2UNIX on $CURR_DIR/deployment/nfs/initial-setup/ SUCCESS"
	else
		error_exit "DOS2UNIX on $CURR_DIR/deployment/nfs/initial-setup/ Failed"
	fi
}
#----------------------------------------------------------------
# PERFORM DOS2UNIX FOR HDFS SCRIPTS
#----------------------------------------------------------------
function perform_0004_dos2unix_hdfs
{
	dos2unix $CURR_DIR/deployment/hdfs/scripts/shell/*.sh; RS_D2U_HDFS=`echo $?`;
	if  [ $RS_D2U_HDFS -eq 0 ]
		then
		success_continue "DOS2UNIX on $CURR_DIR/deployment/hdfs/scripts/shell/ SUCCESS"
	else
		error_exit "DOS2UNIX on $CURR_DIR/deployment/hdfs/scripts/shell/ Failed"
	fi
}
#----------------------------------------------------------------
# COPY HBASE-SITE XML TO HDFS
#----------------------------------------------------------------
function perform_0005_copy_hbasesitexml
{
		cp /usr/hdp/current/hbase-client/conf/hbase-site.xml $CURR_DIR/deployment/hdfs/lib/jars/ ;RS_CPY_HBASESITE=`echo $?`;
		if  [ $RS_CPY_HBASESITE -eq 0 ]
			then
			success_continue "COPYING  /usr/hdp/current/hbase-client/conf/hbase-site.xml TO $CURR_DIR/deployment/hdfs/lib/jars/ SUCCESS"
		else
			error_exit "COPYING  /usr/hdp/current/hbase-client/conf/hbase-site.xml TO $CURR_DIR/deployment/hdfs/lib/jars/ Failed"
		fi
}
#----------------------------------------------------------------
# SET PERMISSION ON NFS
#----------------------------------------------------------------
function perform_0006_unix_permissions
{
	chmod 750 -R $CURR_DIR/deployment/ ; RS_NFS_PRMSN=`echo $?`;
	if  [ $RS_NFS_PRMSN -eq 0 ]
		then
		success_continue "Setting 7-5-0 Permission on $CURR_DIR/deployment/ SUCCESS"
	else
		error_exit "Setting 7-5-0 Permission on $CURR_DIR/deployment/ Failed"
	fi
}
#----------------------------------------------------------------
# DEPLOY COMPONENTS TO HADOOP
#----------------------------------------------------------------
function perform_0007_deploy_to_hadoop
{
	hadoop fs -put -f $CURR_DIR/deployment/hdfs/* /project/scac/$ENV/sourcecode/$SRC_MODULE/; RS_HDFS_PUT=`echo $?`;
	if  [ $RS_HDFS_PUT -eq 0 ]
	then
		success_continue "Transfer of Files from NFS Folder $CURR_DIR/deployment/hdfs/*  TO HDFS Folder /project/scac/$ENV/sourcecode/$SRC_MODULE/ SUCCESS "
	else
		error_exit "Transfer of Files from NFS Folder $CURR_DIR/deployment/hdfs/*  TO HDFS Folder /project/scac/$ENV/sourcecode/$SRC_MODULE/ FAILED "
	fi
}
#----------------------------------------------------------------
# SET PERMISSION ON HDFS
#----------------------------------------------------------------
function perform_0008_hadoop_permissions
{
	hadoop fs -chmod -R 750 /project/scac/$ENV/sourcecode/$SRC_MODULE/; RS_HDFS_PERMSN=`echo $?`;
	if  [ $RS_HDFS_PERMSN -eq 0 ]
	then
		success_continue "Setting 7-5-0 Permission on /project/scac/$ENV/sourcecode/$SRC_MODULE/ SUCCESS ";		
		hadoop fs -ls -R /project/scac/$ENV/sourcecode/$SRC_MODULE/
	else
	error_exit "Setting 7-5-0 Permission on /project/scac/$ENV/sourcecode/$SRC_MODULE/ Failed "
	fi
}
#-------------------------------------------------------------------
# ARCHIVE Deployment.zip in HDFS
#-------------------------------------------------------------------
function perform_0009_archive_deploymentzip_to_hdfs
{
	hadoop fs -put ${DEPLYD_LOCATION_UNIX}/deployment.zip  /project/scac/$ENV/archive/$SRC_MODULE/${CURR_TMSTP}_deployment.zip;
	RS_ZIP_ARCHIVE=`echo $?`;
			if [ $RS_ZIP_ARCHIVE -eq 0 ];then
				success_continue "Archiving deployment.zip to '/project/scac/$ENV/archive/$SRC_MODULE/' successful ";
			else
				echo -e ${COLR_BRT_GREEN} "WARNING !!! WARNING !!! "${COLR_END}
				echo -e ${COLR_BRT_GREEN} "WARNING !!! WARNING !!! - CODE DEPLOYMENT SUCCESSFUL BUT UNABLE TO ARCHIVE THE SOURCE DEPLOYMENT FILE 'deployment.zip' "${COLR_END}
				echo -e ${COLR_BRT_GREEN} "WARNING !!! WARNING !!! - HDFS Location     : '/project/scac/$ENV/archive/$SRC_MODULE/' "${COLR_END}
				echo -e ${COLR_BRT_GREEN} "WARNING !!! WARNING !!! - Zip File for HDFS : '${CURR_TMSTP}_deployment.zip' "${COLR_END};
				echo -e ${COLR_BRT_GREEN} "WARNING !!! WARNING !!! - Zip File in Unix  : '${DEPLYD_LOCATION_UNIX}/deployment.zip' "${COLR_END};
				echo -e ${COLR_BRT_GREEN} "WARNING !!! WARNING !!! "${COLR_END}
			fi
}
#-------------------------------------------------------------------
# PURGE Deployment.zip ON REQUEST BY USER UPON SUCCESFUL DEPLOYMENT
#-------------------------------------------------------------------
function perform_0010_purge_source_successful_deployment
{
	if [ "$DELETE_DEPLOYMENT_ZIP_FILE" == "Y" ]; then
		rm $CURR_DIR/deployment.zip;RS_DELETE_DEPLOYMENT_ZIP_FILE=`echo $?`;
			if [ $RS_DELETE_DEPLOYMENT_ZIP_FILE -eq 0 ];then
				success_continue "PURGING THE SOURCE DEPLOYMENT FILE 'deployment.zip AS REQUESTED BY USER UPON SUCCESSFUL DEPLOYMENT";
			else
				echo -e ${COLR_BRT_GREEN} "WARNING !!! WARNING !!! "${COLR_END}
				echo -e ${COLR_BRT_GREEN} "WARNING !!! WARNING !!! - UNABLE TO PURGE THE SOURCE DEPLOYMENT FILE 'deployment.zip AS REQUESTED BY USER UPON SUCCESSFUL DEPLOYMENT"${COLR_END}
				echo -e ${COLR_BRT_GREEN} "WARNING !!! WARNING !!! "${COLR_END}
			fi	
	fi	
}
#----------------------------------------------------------------
#       MAIN LOGIC FLOW 
#----------------------------------------------------------------
cd $CURR_DIR;

if ([ ! -z $ENV ] && [ ! -z $SRC_MODULE ]);then

		perform_0001_enabling_kerberos;
		perform_0002_unzip_deployment;
		perform_0003_dos2unix_nfs;
		perform_0004_dos2unix_hdfs;
		perform_0005_copy_hbasesitexml;
		perform_0006_unix_permissions;
	
		if   [ $RS_UNZIP -eq 0 ] && [ $RS_D2U_NFS -eq 0 ] && [ $RS_D2U_HDFS -eq 0 ] && [ $RS_CPY_HBASESITE -eq 0 ] && [ $RS_NFS_PRMSN -eq 0 ]
		then
			clear;
			echo $COS_FILLER_85_EQUALS;
			success_continue "NFS Deployement SUCCESSFUL -- PROCEEDING TO DEPLOYING THE CODE IN THE CLUSTER";
			echo $COS_FILLER_85_EQUALS;
		fi
		
		perform_0007_deploy_to_hadoop;
		perform_0008_hadoop_permissions;
		perform_0009_archive_deploymentzip_to_hdfs;
		perform_0010_purge_source_successful_deployment;
		
		if  [ $RS_HDFS_PERMSN -eq 0 ]
		then
			clear;
			echo $COS_FILLER_85_EQUALS;
			success_continue "HADOOP Deployement SUCCESSFUL for ${CURR_TMSTP} -- HAPPY EXECUTING ! ! !";
			echo -e ${COLR_BRT_GREEN}  "$COS_FILLER_85_EQUALS" ${COLR_END};
			echo -e ${COLR_BRT_GREEN} "                    THANK YOU FOR USING THE SCA-C DEPLOYMENT SCRIPT , SEE YA LATER !              "${COLR_END};
			echo -e ${COLR_BRT_GREEN}  "$COS_FILLER_85_EQUALS" ${COLR_END};
		fi	
else
    error_exit "$CURR_USER - Please Enter Correct Parameters"
fi

