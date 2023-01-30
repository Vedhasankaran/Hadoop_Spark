#!/bin/sh
#----------------------------------------------------------------------------------------#
#This script allows the User to perfrom testing (unit/ Integration / End-End) on         #
#the Deployed Components in Unix and HDFS                                                #
#                                                                                        #
#PARAMETERS PASSED  :                                                                    #
#                       1. Environment : Eg:. dev,qa,edu                                 #
#                       2. CCR Number Eg:. 10002 / 10003 /10004                          #
#                       3. Source Module Eg:. aot_vot_polk_vin / paten /save /           #
#                       4. Process Type Eg: ADHOC / REGULAR                              #
#                                                                                        #
#----------------------------------------------------------------------------------------#
#                           M O D I F I C A T I O N         L O G                        #
#----------------------------------------------------------------------------------------#
#       AUTHOR                       DESCRIPTION                                 DATE    #
#  ------------         ----------------------------------------------- -----------------#
#  HVEDHASA                     INITIAL VERSION                         10-JAN-2017      #
#                                                                                        #
#----------------------------------------------------------------------------------------#
#!/bin/sh
echo -e "\033c\e[3J";

echo -n "Enter your ROLE  (ex : DEV --> Developer /Migrators/, QC --> Testers ) :" 
read PERSON_ROLE;
PERSON_ROLE_UC=`echo $PERSON_ROLE | tr '[:lower:]' '[:upper:]'`

if ([ "$PERSON_ROLE_UC" != "DEV" ] && [ "$PERSON_ROLE_UC" != "QC" ]);then
	echo "the Entered Role is  -->" $PERSON_ROLE_UC;
	echo "Enter Correct Role i.e. DEV or  QC  ) :"
	exit 1;
else
	echo "the Role entered is -->" $PERSON_ROLE_UC;
	clear;
fi

if ([ "$PERSON_ROLE_UC" == "DEV" ]);then
	echo -n "Enter the Environement (dev/qa/edu): " 
	read ENV1
	ENV_LC=`echo $ENV1 | tr '[:upper:]' '[:lower:]'`
	ENV_UC=`echo $ENV1 | tr '[:lower:]' '[:upper:]'`
	if [ "x$ENV1" == "x" ];then
	   echo "Please enter valid Environement (dev/qa/edu) !"
	   echo ""
	   exit 1
	else
		if ([ "$ENV_LC" != "dev" ] && [ "$ENV_LC" != "qa" ] && [ "$ENV_LC" != "edu" ] \
			&& [ "$ENV_UC" != "DEV" ] && [ "$ENV_UC" != "QA" ] && [ "$ENV_UC" != "EDU" ]);then
			echo "Please choose valid Environement (dev/qa/edu) !"
			echo ""
			exit 1
		fi
	fi

	echo ""

	echo -n "Enter CCR Num (eg: 10002 / 10004  ) : " 
	read SRC_MODULE_CCR 
	if [ "x$SRC_MODULE_CCR" == "x" ];then
	   echo "Please enter valid CCR Num (eg: 10002 / 10004  ) :"
	   echo ""
	   exit 1
	fi

	echo ""

	echo -n "Enter Source Name without CCR (eg: sls / aot_vot_polk_vin ) : " 
	read SRC_MODULE1
	SRC_MODULE_UC=`echo $SRC_MODULE1 | tr '[:lower:]' '[:upper:]'`
	SRC_MODULE_LC=`echo $SRC_MODULE1 | tr '[:upper:]' '[:lower:]'`
	if [ "x$SRC_MODULE1" == "x" ];then
	   echo "Please enter valid Appication (eg: sls / aot_vot_polk_vin ) !"
	   echo ""
	   exit 1
	fi

	echo ""

	echo -n "Enter Process Type (eg: REGULAR or ADHOC) : " 
	read PROCESS_TYPE1
	PROCESS_TYPE_UC=`echo $PROCESS_TYPE1 | tr '[:lower:]' '[:upper:]'`
	PROCESS_TYPE_LC=`echo $PROCESS_TYPE1 | tr '[:upper:]' '[:lower:]'`
	if [ "x$PROCESS_TYPE1" == "x" ];then
	   echo "Please enter valid Process Type (eg: REGULAR or ADHOC) !"
	   exit 1
	else
		if ([ "$PROCESS_TYPE_LC" != "regular" ] && [ "$PROCESS_TYPE_LC" != "adhoc" ] \
			&& [ "$PROCESS_TYPE_UC" != "REGULAR" ] && [ "$PROCESS_TYPE_UC" != "ADHOC" ] );then
			echo "Please choose valid valid Process Type (eg: REGULAR or ADHOC) !"
			echo ""
			exit 1
		fi
	fi

	echo ""

	echo -n "Enter Unix Location of the Deployment Folder (eg: /s/iapxhtam/POLK/vin/ ) : " 
	read DEPLYD_LOCATION_UNIX
	if [ "x$DEPLYD_LOCATION_UNIX" == "x" ];then
	   echo "Please enter valid Process Type (eg: REGULAR or ADHOC) !"
	   exit 1
	else
		if [ -d "$DEPLYD_LOCATION_UNIX/deployment/" ];then
			echo "";
		else
			echo " !!!! ERROR !!! ERRROR  "
			echo " !!!! ERROR !!! ERRROR - Folder '$DEPLYD_LOCATION_UNIX/deployment/' doesn't not exist "
			echo " !!!! ERROR !!! ERRROR  "
			echo ""
			exit 1;
		fi
	fi

	echo ""

	echo -n "Enter Delta Version (eg: 2.4.2.0-258 or 2.5.3.0-37 ) : " 
	read DELTA_SPARK_VERSION;
	echo ""

	echo -n "Enter Dedupe Version (eg: 2.4.2.0-258 or 2.5.3.0-37 ) : " 
	read DEDUPE_SPARK_VERSION;
	echo ""

	echo -n "Enter 'YOUR' FORD CDSID : " 
	read FORD_USER_CDSID
	FORD_USER_CDSID_LC=`echo $FORD_USER_CDSID | tr '[:upper:]' '[:lower:]'`
	FORD_USER_CDSID_UC=`echo $FORD_USER_CDSID | tr '[:lower:]' '[:upper:]'`
	if [ "x$FORD_USER_CDSID" == "x" ];then
	   echo "Please enter valid FORD CDSID !!"
	   echo ""
	   exit 1
	fi

	echo ""
fi
clear;
#---------------------------------------------------------------
#variable Declaration
#---------------------------------------------------------------
PROGNAME=$(basename $0)
CURR_SERVER=`hostname -s`;
CURR_USER=`whoami`;
CURR_DIR=`pwd`
FALCON_DIR=$DEPLYD_LOCATION_UNIX/deployment/nfs/initial-setup/;
JAR_DIR=$DEPLYD_LOCATION_UNIX/deployment/hdfs/lib/jars/;
FALCON_DIR_MASTERDELETE='*aster*elete.sh';
FALCON_DIR_MASTERKICKOFF='*aster*ff.sh';
BATCH_JAR='*batch*.jar';
EXTRACT_JAR='*extract*.jar';
DELTA_JAR='*delta*.jar';
DEDUPE_JAR='*dedup*.jar';
CCR_APP=$SRC_MODULE_CCR'_'$SRC_MODULE_LC;
COS_FILLER1="::::::::::::::::::*************************************************************************************************:::::::::::::::::";
COS_FILLER2=" ";
COS_FILLER3="------------------------------------------------------------------------------------";
MSDP01_FIELDS="rowkey,applicationname,loadaction,processdatecode,status,lastupdton,lastupdtby";
MSDP02_FIELDS="rowkey,applicationname,processdatecode,workflow,starttime,endtime,status,lastupdton,lastupdtby";
MSDP04_FIELDS="rowkey,applicationname,loadaction,loadgroup,processdatecode,tablename,sourcezone,targetzone,status,startdatetime,srcrowcnt,enddatetime,trgtrowcnt,jobid,errmessage,lastupdton,lastupdtby";

FULL_SRC_UC="${SRC_MODULE_CCR}_${SRC_MODULE_UC}";
FULL_SRC_LC="${SRC_MODULE_CCR}_${SRC_MODULE_LC}";
UOOOU="_";
US="_";
DSC_FW_STATUS="DSC_COMPLETED";
TO_EMAIL=$FORD_USER_CDSID_LC@ford.com;

LOG_FILE_NAME="${PROGNAME}_log_${ENV_LC}_${FULL_SRC_LC}.txt";
LOGGER_INPUT='PROGRAM STARTED';

CONST_LZ="LANDING_ZONE|"
CONST_PRE="PRE-STAGING|"
CONST_POST="POST-STAGING|";
CONST_T1="T1-ZONE|";

export red='\e[0;31m'
export bred='\e[1;31m'
export ured='\e[4;31m'
export nc='\e[0m' 
export yellow='\e[1;33m'
export bblue='\033[1;34m'
export bgreen='\033[1;32m'

DB_TBL_FEED_FILE=${CURR_DIR}/input.txt;
DB_TBL_STATS_FILE=${CURR_DIR}/new.csv


#----------------------------------------------------------------
# FUNCTION CALLING : Invoking the Main Menu
#----------------------------------------------------------------
function perform_menu_calling
{
LOGGER_INPUT="Selecting Main Testing Option";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	echo $COS_FILLER2;
	echo $COS_FILLER3;
	echo "  *  *  SCA-C TESTING MENU FOR $CCR_APP  IN $ENV_LC  *  * "
	echo "            Enter your Option ( 1 till 4 )"
	echo $COS_FILLER2;
	echo "	1.Jar Unit Testing"
	echo $COS_FILLER2;
	echo "	2.Jar Integration Testing"
	echo $COS_FILLER2;
	echo "	3.Falcon Integration Testing"
	echo $COS_FILLER2;
	echo "	4.Environment Reset to Ground Zero state"
	echo $COS_FILLER2;
	echo "	5.Exit"
	echo $COS_FILLER2;
	echo " NOTE : any Option other than the above will result in repeating the MAIN Menu"
	echo $COS_FILLER3;
	echo $COS_FILLER2;
	echo -en ${yellow}" Enter your Option ( 1 till 5) : " ${nc} ;
	read MAIN_TEST_OPTION_MENU
	echo $COS_FILLER2;

	if [ "$MAIN_TEST_OPTION_MENU" == "2" ];then
	echo "Entered Main Menu Option is $MAIN_TEST_OPTION_MENU!"
	perform_jarintegration_testing;
	perform_menu_calling;
	fi
	
	if [ "$MAIN_TEST_OPTION_MENU" == "1" ];then
	echo "Entered Main Menu Option is $MAIN_TEST_OPTION_MENU!"
	perform_unit_testing;
	perform_menu_calling;
	fi
	
	if [ "$MAIN_TEST_OPTION_MENU" == "3" ];then
	echo "Entered Main Menu Option is $MAIN_TEST_OPTION_MENU!"
	perform_falcontesting;
	perform_menu_calling;
	fi
	
	if [ "$MAIN_TEST_OPTION_MENU" == "4" ];then
	echo "Entered Main Menu Option is $MAIN_TEST_OPTION_MENU!"
	perform_menu_calling_auditstats_and_envreset;
	perform_menu_calling;
	fi
	
	if [ "$MAIN_TEST_OPTION_MENU" == "5" ];then
	echo "Entered Main Menu Option is $MAIN_TEST_OPTION_MENU!"
	exit 0
	fi
	
	if [ "$MAIN_TEST_OPTION_MENU" != "1" ] || [ "$MAIN_TEST_OPTION_MENU" != "2" ] \
	|| [ "$MAIN_TEST_OPTION_MENU" != "3" ] || [ "$MAIN_TEST_OPTION_MENU" != "4" ] \
	|| [ "x$MAIN_TEST_OPTION_MENU" == "x" ] ;then
	clear;
	echo ""
#	echo "INVALID OPTION ENTERED IN MAIN MENU ($MAIN_TEST_OPTION_MENU)-- Please Enter the Appropriate Option Specified !! "
	perform_menu_calling;
	fi
}
#----------------------------------------------------------------
# FUNCTION CALLING :Selecting Unit Testing Option
#----------------------------------------------------------------
function perform_unit_testing
{
LOGGER_INPUT="Selecting Unit Testing Option";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	echo $COS_FILLER2;
	echo $COS_FILLER3;
	echo "	SCA-C UNIT Testing MENU for $CCR_APP  in $ENV_LC "
	echo "      Enter your Unit Testing Option ( 1 till 6)"
	echo ""
	echo "	1.BATCH OPEN"
	echo "	2.DATA EXTRACT"
	echo "	3.DELTA"
	echo "	4.DEDUPE"
	echo "	5.BATCH CLOSE"
	echo "	6.MAIN MENU"
	echo $COS_FILLER2;
	echo " NOTE : any Option other than the above will result in repeating the Unit Testing Menu"
	echo $COS_FILLER3;
	echo $COS_FILLER2;
	echo -en ${yellow} "Enter your Unit Testing Option ( 1 till 6) : " ${nc}
	read OPTION_MENU_UNIT_TESTING
	echo $COS_FILLER2;
	
	if [ "$OPTION_MENU_UNIT_TESTING" == "1" ];then
	echo "Entered Option for UNIT TESTING is $OPTION_MENU_UNIT_TESTING"
	echo "Proceeding to UNIT Test BATCH OPEN for $CCR_APP in $ENV_LC "
	echo $COS_FILLER2;
	BATCH_ACTION='start';
	perform_jarunit_testing_batch $PROCESS_TYPE_LC $BATCH_ACTION;
	fi	
	
	if [ "$OPTION_MENU_UNIT_TESTING" == "2" ];then
	echo "Entered Option for UNIT TESTING is $OPTION_MENU_UNIT_TESTING"
	echo "Proceeding to UNIT Test DATA-EXTRACT for $CCR_APP in $ENV_LC "
	echo $COS_FILLER2;
	perform_jarunit_testing_extract $PROCESS_TYPE_LC;
	fi	
	
	if [ "$OPTION_MENU_UNIT_TESTING" == "3" ];then
	echo "Entered Option for UNIT TESTING is $OPTION_MENU_UNIT_TESTING"
	echo "Proceeding to UNIT Test DELTA for $CCR_APP in $ENV_LC "
	echo $COS_FILLER2;
	perform_jarunit_testing_delta $PROCESS_TYPE_LC;
	fi	
	
	if [ "$OPTION_MENU_UNIT_TESTING" == "4" ];then
	echo "Entered Option for UNIT TESTING is $OPTION_MENU_UNIT_TESTING"
	echo "Proceeding to UNIT Test DEDUPE for $CCR_APP in $ENV_LC "
	echo $COS_FILLER2;
	perform_jarunit_testing_dedupe $PROCESS_TYPE_LC;
	fi	
	
	if [ "$OPTION_MENU_UNIT_TESTING" == "5" ];then
	echo "Entered Option for UNIT TESTING is $OPTION_MENU_UNIT_TESTING"
	echo "Proceeding to UNIT Test BATCH CLOSE for $CCR_APP in $ENV_LC "
	echo $COS_FILLER2;
	BATCH_ACTION='complete';
	perform_jarunit_testing_batch $PROCESS_TYPE_LC $BATCH_ACTION;
	fi	
	
	if [ "$OPTION_MENU_UNIT_TESTING" == "6" ];then
	echo "Entered Option for UNIT TESTING is $OPTION_MENU_UNIT_TESTING"
	perform_menu_calling;
	fi
	
	if  [ "$OPTION_MENU_UNIT_TESTING" != "1" ] || [ "$OPTION_MENU_UNIT_TESTING" != "2" ] \
	||  [ "$OPTION_MENU_UNIT_TESTING" != "3" ] || [ "$OPTION_MENU_UNIT_TESTING" != "4" ] \
	||  [ "$OPTION_MENU_UNIT_TESTING" != "5" ] || [ "$OPTION_MENU_UNIT_TESTING" != "6" ];then
	clear;
	echo $COS_FILLER2;
#	echo "INVALID OPTION Entered or UNIT TESTING ($OPTION_MENU_UNIT_TESTING) -- Please Enter the Appropriate Unit Testing ";
	perform_unit_testing;
	fi
}
#----------------------------------------------------------------
# FUNCTION CALLING :Selecting Falcon Testing Option
#----------------------------------------------------------------
function perform_falcontesting
{
LOGGER_INPUT="Selecting Falcon Testing Option";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	echo $COS_FILLER2;
	echo $COS_FILLER3;
	echo "	SCA-C FALCON Testing MENU for $CCR_APP  in $ENV_LC "
	echo "     Enter your Falcon Testing Option ( 1 till 3 )"
	echo ""
	echo "	1.MASTER DELETE  - Purge Falcons from Server"
	echo "	2.MASTER KICKOFF - Submit To & Execute Falcons from Server"
	echo "	3.MAIN MENU"
	echo $COS_FILLER2;
	echo " NOTE : any Option other than the above will result in repeating the Falcon Testing Menu"
	echo $COS_FILLER3;
	echo $COS_FILLER2;
	echo -en "Enter your Falcon Testing Option ( 1 till 3 ) : "
	
	read OPTION_MENU_FACLON_TESTING
	echo $COS_FILLER2;
	
	if [ "$OPTION_MENU_FACLON_TESTING" == "1" ];then
		echo "Entered Option for FACLON TESTING is $OPTION_MENU_FACLON_TESTING"
		echo "Proceeding to PURGE FALCON FROM SERVER for $CCR_APP in $ENV_LC "
		echo $COS_FILLER2;
		perform_falcontesting_master_delete;
	fi

	if [ "$OPTION_MENU_FACLON_TESTING" == "2" ];then
		echo "Entered Option for FACLON TESTING is $OPTION_MENU_FACLON_TESTING"
		echo "Proceeding to KICKOFF FALCON FROM SERVER for $CCR_APP in $ENV_LC "
		echo $COS_FILLER2;
		perform_falcontesting_master_delete; PID32=$!;
		wait $PID32;
		perform_falcontesting_master_kickoff;
	fi	

	if [ "$OPTION_MENU_FACLON_TESTING" == "3" ];then
		echo "Entered Option for FACLON TESTING is $OPTION_MENU_FACLON_TESTING"
		echo "Proceeding to MAIN-MENU for $CCR_APP in $ENV_LC "
		echo $COS_FILLER2;
		perform_menu_calling;
	fi

	if  [ "$OPTION_MENU_FACLON_TESTING" != "1" ] || [ "$OPTION_MENU_FACLON_TESTING" != "2" ] \
	||  [ "$OPTION_MENU_FACLON_TESTING" != "3" ] || [ "x$OPTION_MENU_FACLON_TESTING" != "x" ];then
	clear;
	echo $COS_FILLER2;
#	echo "INVALID OPTION Entered or FALCON TESTING ($OPTION_MENU_UNIT_TESTING) -- Please Enter the Appropriate Falcon Testing Option ";
	perform_falcontesting;
	fi	
}
#----------------------------------------------------------------
# FUNCTION CALLING : Jar Integration Testing
#----------------------------------------------------------------
function perform_jarintegration_testing
{
LOGGER_INPUT="Jar Integration Testing menu";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	echo $COS_FILLER2;
	echo $COS_FILLER3;
	echo "	SCA-C JAR INTERGRATION Testing MENU for $CCR_APP  in $ENV_LC "
	echo "     Enter your Jar Integration Testing Option ( 1 till 3 )"
	echo ""
	echo "	1.PRE-HADOOP  PROCESS - Batch-Open/Data-Extract/Delta"
	echo "	2.POST-HADOOP PROCESS - Dedupe/Batch-Close"
	echo "	3.MAIN MENU"
	echo $COS_FILLER2;
	echo " NOTE : any Option other than the above will result in repeating the Jar Integration Testing Menu"
	echo $COS_FILLER3;
	echo $COS_FILLER2;
	echo -n "Enter your Jar Integration Testing Option ( 1 till 3 ) : " 
	read OPTION_MENU_JARINT_TESTING
	echo $COS_FILLER2;
	
	if [ "$OPTION_MENU_JARINT_TESTING" == "1" ];then
		echo "Entered Option for JAR INTEGRATION TESTING is $OPTION_MENU_JARINT_TESTING"
		echo "Proceeding to INITIATE PRE-HADOOP TESTING for $CCR_APP in $ENV_LC "
		echo $COS_FILLER2;
		perform_filewatcher_check;
		perform_jarintegration_testing_prehadoop;
	fi
	
	if [ "$OPTION_MENU_JARINT_TESTING" == "2" ];then
		echo "Entered Option for JAR INTEGRATION TESTING is $OPTION_MENU_JARINT_TESTING"
		echo "Proceeding to INITIATE POST-HADOOP TESTING for $CCR_APP in $ENV_LC "
		echo $COS_FILLER2;
		perform_filewatcher_check;
		perform_jarintegration_testing_posthadoop;
	fi
	
	if [ "$OPTION_MENU_JARINT_TESTING" == "3" ];then
		echo "Entered Option for JAR INTEGRATION TESTING is $OPTION_MENU_JARINT_TESTING"
		echo "Proceeding to MAIN-MENU for $CCR_APP in $ENV_LC "
		echo $COS_FILLER2;
		perform_menu_calling;
	fi
	
	if  [ "$OPTION_MENU_JARINT_TESTING" != "1" ] || [ "$OPTION_MENU_JARINT_TESTING" != "2" ] \
	||  [ "$OPTION_MENU_JARINT_TESTING" != "3" ] || [ "x$OPTION_MENU_JARINT_TESTING" != "x" ];then
	clear;
	echo $COS_FILLER2;
#	echo "INVALID OPTION Entered or FALCON TESTING ($OPTION_MENU_JARINT_TESTING) -- Please Enter the Appropriate Falcon Testing Option ";
	perform_jarintegration_testing;
	fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Jar Integration Testing : Pre-Hadoop
#----------------------------------------------------------------
function perform_jarintegration_testing_prehadoop
{
LOGGER_INPUT="Jar Integration Testing : Pre-Hadoop";

perform_user_activity_log ${LOGGER_INPUT};

	if [ "$RS_FW_DSC" == "0" ];then
#		perform_audit_msdp01_batch;
#--		
		if [ "$RS_FW_BATCH" == "0" ];then
			echo "BATCH OPEN DONE - Hence Skipping Batch Open  !" & PID111=$!
		else
			echo "BATCH-OPEN NOT DONE !"
			BATCH_ACTION='start';
			perform_jarunit_testing_batch $PROCESS_TYPE_LC $BATCH_ACTION & PID111=$!
		fi
#--		
		wait $PID111

		if [ "$RS_FW_EXTRACT" == "0" ];then
			echo "EXTRACT DONE - Hence Skipping Extract  !" & PID112=$!
		else
			echo "EXTRACT NOT DONE !"
			perform_jarunit_testing_extract $PROCESS_TYPE_LC & PID112=$!;
		fi
#--	
		wait $PID111 $PID112
		
		if [ "$RS_FW_DELTA" == "0" ];then
			echo "DELTA DONE - Hence Skipping Delta !" & PID3=$!
		else
			echo "DELTA NOT DONE !"
			perform_jarunit_testing_delta $PROCESS_TYPE_LC & PID113=$!;
		fi
#--	
		wait $PID111 $PID112 $PID113
	else
		echo $COS_FILLER1;
		echo $COS_FILLER2
		echo "DSC File Watcher '${SRC_MODULE_UC}_DSC_COMPLETED' is not avaliable !! in the HDFS Location : /project/scac/$ENV_LC/filewatcher/$CCR_APP/ "
		echo $COS_FILLER2
		echo "Hence PRE-HADOOP Intergration Testing Skipped for  for $CCR_APP in $ENV_LC "
		echo $COS_FILLER2
		echo $COS_FILLER1;
	fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Jar Integration Testing : Post-Hadoop
#----------------------------------------------------------------
function perform_jarintegration_testing_posthadoop
{	
LOGGER_INPUT="Jar Integration Testing : Post-Hadoop";

perform_user_activity_log ${LOGGER_INPUT};

	if [ "$RS_FW_DATASTAGE" == "0" ];then
#--	
		if [  "$RS_FW_DEDUPE" == "0" ];then
			echo "DEDUPE DONE !" & PID121=$!
		else
			echo "DEDUPE NOT DONE !"
			perform_jarunit_testing_dedupe $PROCESS_TYPE_LC & PID121=$!;
		fi	
	#--	
		wait $PID121
				
		perform_filewatcher_check;
		
		if ([ "$RS_FW_DEDUPE" == "0" ] && [ "$RS_FW_DATASTAGE" == "0" ] \
		&& [ "$RS_FW_DELTA" == "0" ]  && [ "$RS_FW_EXTRACT" == "0" ] \
		&& [ "$RS_FW_BATCH" == "0" ]);then
			BATCH_ACTION='complete';
			perform_jarunit_testing_batch $PROCESS_TYPE_LC $BATCH_ACTION;
		else
			echo "BATCH COMPELETE NOT DONE !"
		fi
	else
		echo $COS_FILLER1;
		echo $COS_FILLER2
		echo "DATASTAGE File Watcher 'DS_COMPLETED' is not avaliable !! in the HDFS Location : /project/scac/$ENV_LC/filewatcher/$CCR_APP/ "
		echo $COS_FILLER2
		echo "Hence POST-HADOOP Intergration Testing Skipped for  for $CCR_APP in $ENV_LC "
		echo $COS_FILLER2
		echo $COS_FILLER1;
	fi	
}
#----------------------------------------------------------------
# FUNCTION CALLING :Unit Testing Batch-Open/Close
#----------------------------------------------------------------
function perform_jarunit_testing_batch
{
LOGGER_INPUT="Executing Batch-Open/Close";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	cd $JAR_DIR;
	hadoop jar $BATCH_JAR ${1} ${2};
	echo $COS_FILLER2;
}
#----------------------------------------------------------------
# FUNCTION CALLING :Unit Testing Data-Extract
#----------------------------------------------------------------
function perform_jarunit_testing_extract
{
LOGGER_INPUT="Executing Data-Extract";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	cd $JAR_DIR;
	hadoop jar $EXTRACT_JAR ${1};
}
#----------------------------------------------------------------
# FUNCTION CALLING :Unit Testing Delta
#----------------------------------------------------------------
function perform_jarunit_testing_delta
{
LOGGER_INPUT="Executing Delta";

perform_user_activity_log ${LOGGER_INPUT};

if [ "x$DELTA_SPARK_VERSION" != "x" ]; then
	DELTA_SPARK_VERSION_SUBMIT="/usr/hdp/${DELTA_SPARK_VERSION}/spark/bin/spark-submit";
else
	DELTA_SPARK_VERSION_SUBMIT="spark-submit";
fi;

	clear;
	cd $JAR_DIR;
	export HbaseConf=/etc/hbase/conf/:
	${DELTA_SPARK_VERSION_SUBMIT} \
	--conf spark.sql.hive.convertMetastoreOrc=false \
	--files /etc/hive/conf/hive-site.xml \
	--driver-class-path $HbaseConf \
	--master yarn  \
	--deploy-mode client \
	--num-executors 6 \
	--executor-memory 6g \
	--jars ./Backported-UDFs-0.0.1.jar,./spark-csv_2.10-1.3.0.jar,./commons-csv-1.1.jar \
	--queue scac ./$DELTA_JAR ${1};
}
#----------------------------------------------------------------
# FUNCTION CALLING :Unit Testing Dedupe
#----------------------------------------------------------------
function perform_jarunit_testing_dedupe
{
LOGGER_INPUT="Executing Dedupe";

perform_user_activity_log ${LOGGER_INPUT};

if [ "x$DEDUPE_SPARK_VERSION" != "x" ]; then
	DEDUPE_SPARK_VERSION_SUBMIT="/usr/hdp/${DEDUPE_SPARK_VERSION}/spark/bin/spark-submit";
else
	DEDUPE_SPARK_VERSION_SUBMIT="spark-submit";
fi;

	clear;
	cd $JAR_DIR;
	export HbaseConf=/etc/hbase/conf/:
	${DEDUPE_SPARK_VERSION_SUBMIT} \
	--conf spark.sql.hive.convertMetastoreOrc=false \
	--files /etc/hive/conf/hive-site.xml \
	--jars ./Backported-UDFs-0.0.1.jar,./spark-csv_2.10-1.3.0.jar,./commons-csv-1.1.jar \
	--driver-class-path $HbaseConf \
	--master yarn \
	--deploy-mode client \
	--num-executors 6 \
	--executor-memory 6g \
	--queue scac ./$DEDUPE_JAR ${1};
}
#----------------------------------------------------------------
# FUNCTION CALLING :Execute Master Delete Falcons
#----------------------------------------------------------------
function perform_falcontesting_master_delete
{
LOGGER_INPUT="Execute Master Delete Falcons";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	echo "PERFORMING PURGING OF FALCONS FROM SERVER FOR END-END TESTING FOR $CCR_APP in $ENV_LC "
	cd $FALCON_DIR;
	echo $COS_FILLER2;
	echo $COS_FILLER2;
	echo $COS_FILLER2;
	echo $COS_FILLER2;
	sh $FALCON_DIR_MASTERDELETE;
}
#----------------------------------------------------------------
# FUNCTION CALLING :Execute Master Kickoff Falcons
#----------------------------------------------------------------
function perform_falcontesting_master_kickoff
{
LOGGER_INPUT="Execute Master Kickoff Falcons";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	echo "PERFORMING FALCON END-END TESTING FOR $CCR_APP in $ENV_LC "
	cd $FALCON_DIR;
	echo $COS_FILLER2;
	echo $COS_FILLER2;
	echo $COS_FILLER2;
	echo $COS_FILLER2;		
	sh $FALCON_DIR_MASTERKICKOFF;
}
#----------------------------------------------------------------
# FUNCTION CALLING : Retreive File Watcher Information
#----------------------------------------------------------------
function perform_filewatcher_check
{
LOGGER_INPUT="Retreive File Watcher Information";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	hadoop fs -test -e /project/scac/$ENV_LC/filewatcher/$CCR_APP/${SRC_MODULE_UC}_DSC_COMPLETED;RS_FW_DSC=`echo $?`;
	hadoop fs -test -e /project/scac/$ENV_LC/filewatcher/$CCR_APP/REGULAR_MASTERSTART_COMPLETED;RS_FW_BATCH=`echo $?`;
	hadoop fs -test -e /project/scac/$ENV_LC/filewatcher/$CCR_APP/EXTRACT_COMPLETED;RS_FW_EXTRACT=`echo $?`;
	hadoop fs -test -e /project/scac/$ENV_LC/staging_zone/$CCR_APP/delta/*_${SRC_MODULE_LC}_delta.txt;RS_FW_DELTA=`echo $?`;
	hadoop fs -test -e /project/scac/$ENV_LC/filewatcher/$CCR_APP/DS_COMPLETED;RS_FW_DATASTAGE=`echo $?`;
	hadoop fs -test -e /project/scac/$ENV_LC/filewatcher/$CCR_APP/DEDUP_COMPLETED;RS_FW_DEDUPE=`echo $?`;
}
#----------------------------------------------------------------
# FUNCTION CALLING : Retreive Audit 04-Batch Table of Extract
#----------------------------------------------------------------
function perform_audit_msdp04_data_ingestion_status_extract
{
LOGGER_INPUT="Retreive Audit 04-Batch Table of Extract";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	
	MSDP04_BATCH_EXTRACT_LINE1="SELECT $MSDP04_FIELDS FROM scac_"$ENV_LC"_ops_db.msdp04_data_ingestion_status ";
	MSDP04_BATCH_EXTRACT_LINE2=" WHERE applicationname = '$SRC_MODULE_UC'";
	MSDP04_BATCH_EXTRACT_LINE3=" and processdatecode = '$MSDP01_BATCH_PROCESSDATECODE' ";
	MSDP04_BATCH_EXTRACT_LINE4=" and loadaction = 'EXTRACT' ";
	MSDP04_BATCH_EXTRACT_QUERY=" $MSDP04_BATCH_EXTRACT_LINE1 $MSDP04_BATCH_EXTRACT_LINE2 $MSDP04_BATCH_EXTRACT_LINE3 $MSDP04_BATCH_EXTRACT_LINE4";
	MSDP04_BATCH_EXTRACT=`hive -e " $MSDP04_BATCH_EXTRACT_QUERY "| sed -e 's/ //g' | sed -e 's/\t/|/g'`;
	MSDP04_BATCH_EXTRACT_STATUS=`echo $MSDP04_BATCH_EXTRACT | awk 'BEGIN{FS="|"} {print $9}'`;
}
#----------------------------------------------------------------
# FUNCTION CALLING : Retreive Audit 04-Batch Table of Delta
#----------------------------------------------------------------
function perform_audit_msdp04_data_ingestion_status_delta
{
LOGGER_INPUT="Retreive Audit 04-Batch Table of Delta";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	MSDP04_BATCH_DELTA_LINE1="SELECT $MSDP04_FIELDS FROM scac_"$ENV_LC"_ops_db.msdp04_data_ingestion_status ";
	MSDP04_BATCH_DELTA_LINE2=" WHERE applicationname = '$SRC_MODULE_UC' ";
	MSDP04_BATCH_DELTA_LINE3=" and processdatecode = '$MSDP01_BATCH_PROCESSDATECODE' ";
	MSDP04_BATCH_DELTA_LINE4=" and loadaction = 'DELTA' ";
	MSDP04_BATCH_DELTA_QUERY="$MSDP04_BATCH_DELTA_LINE1 $MSDP04_BATCH_DELTA_LINE2 $MSDP04_BATCH_DELTA_LINE3 $MSDP04_BATCH_DELTA_LINE4";
	MSDP04_BATCH_DELTA=`hive -e " $MSDP04_BATCH_DELTA_QUERY "| sed -e 's/ //g' | sed -e 's/\t/|/g'`;
	MSDP04_BATCH_DELTA_STATUS=`echo $MSDP04_BATCH_DELTA | awk 'BEGIN{FS="|"} {print $9}'`;
}
#----------------------------------------------------------------
# FUNCTION CALLING : Retreive Audit 04-Batch Table of Dedupe
#----------------------------------------------------------------
function perform_audit_msdp04_data_ingestion_status_dedupe
{
LOGGER_INPUT="Retreive Audit 04-Batch Table of Dedupe";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	MSDP04_BATCH_DEDUPE_LINE1="SELECT $MSDP04_FIELDS FROM scac_"$ENV_LC"_ops_db.msdp04_data_ingestion_status ";
	MSDP04_BATCH_DEDUPE_LINE2=" WHERE applicationname = '$SRC_MODULE_UC' ";
	MSDP04_BATCH_DEDUPE_LINE3=" and processdatecode = '$MSDP01_BATCH_PROCESSDATECODE' ";
	MSDP04_BATCH_DEDUPE_LINE4=" and loadaction = 'DEDUPE' ";
	MSDP04_BATCH_DEDUPE_QUERY="$MSDP04_BATCH_DEDUPE_LINE1 $MSDP04_BATCH_DEDUPE_LINE2 $MSDP04_BATCH_DEDUPE_LINE3 $MSDP04_BATCH_DEDUPE_LINE4";
	MSDP04_BATCH_DEDUPE=`hive -e " $MSDP04_BATCH_DEDUPE_QUERY "| sed -e 's/ //g' | sed -e 's/\t/|/g'`;
	MSDP04_BATCH_DEDUPE_STATUS=`echo $MSDP04_BATCH_DEDUPE | awk 'BEGIN{FS="|"} {print $9}'`;
}
#----------------------------------------------------------------
# FUNCTION CALLING : Checks for the current executor and decides
#                    to procced or terminate the execution
#----------------------------------------------------------------
function perform_menu_calling_auditstats_and_envreset
{
	clear;
	echo $COS_FILLER2;
	echo $COS_FILLER3;
	echo "      SCA-C   ENVIRONMENT   RESET   MENU "
	echo "			FOR"
	echo "	  APPLICATION  -> $FULL_SRC_LC ";
	echo "	  ENVIRONMENT  -> $ENV_LC ";
	echo "	  PROCESS TYPE -> $PROCESS_TYPE_UC";
	echo $COS_FILLER2;
	#echo " Enter your Option ( 1 or 2 or 3 or 4 or 5 or 6 or 7)"
	echo $COS_FILLER2;
	echo "	1.Reset HDFS Filewatchers" ;
	echo $COS_FILLER2;
	echo "	2.Reset Audit Entries";
	echo $COS_FILLER2;
	echo "	3.Truncate All T1 Tables ";
	echo $COS_FILLER2;
	echo "	4.Truncate All POST-STAGING Tables";
	echo $COS_FILLER2;
	echo "	5.Truncate All PRE-STAGING Tables";
	echo $COS_FILLER2;
	echo "	6.Truncate ANY SPECIFIC Table";
	echo $COS_FILLER2;
	echo "	7.Create Audit Table Commands for Manual Entry";
	echo $COS_FILLER2;
	echo "	8.Retreive/Email Audit Statistics"
	echo $COS_FILLER2;
	echo "	9.Retreive/Email Count Statistics for SINGLE-FLOW TABLE"
	echo $COS_FILLER2;
	echo "	10.Retreive/Email Audit Statistics for MULTIPLE TABLE"
	echo $COS_FILLER2;
	echo "	11.Return to Main Menu"
	echo $COS_FILLER2;
	echo " NOTE : any Option other than the above will result in repeating the MAIN Menu"
	echo $COS_FILLER3;
	echo $COS_FILLER2;
	echo -n " Enter your Option ( Between 1 and 11 ONLY !!) : " 
	read RESET_OPTION_MENU
	echo $COS_FILLER2;

	if [ "$RESET_OPTION_MENU" == "1" ];then
	echo "Entered Main Menu Option is $RESET_OPTION_MENU!"
	perform_reset_file_watchers;
	perform_menu_calling_auditstats_and_envreset;
	fi
	
	if [ "$RESET_OPTION_MENU" == "2" ];then
	echo "Entered Main Menu Option is $RESET_OPTION_MENU!"
	perform_audit_msdp01_batch;
	perform_retreive_rowkeys;
	perform_reset_audit_tables;
	perform_menu_calling_auditstats_and_envreset;
	fi
	
	if [ "$RESET_OPTION_MENU" == "3" ];then
	echo "Entered Main Menu Option is $RESET_OPTION_MENU!"
	perform_truncate_t1;
	perform_menu_calling_auditstats_and_envreset;
	fi
	
	if [ "$RESET_OPTION_MENU" == "4" ];then
	echo "Entered Main Menu Option is $RESET_OPTION_MENU!"
	perform_truncate_post_staging;
	perform_menu_calling_auditstats_and_envreset;
	fi
		
	if [ "$RESET_OPTION_MENU" == "5" ];then
	echo "Entered Main Menu Option is $RESET_OPTION_MENU!"
	perform_truncate_pre_staging;
	perform_menu_calling_auditstats_and_envreset;
	fi
	
	if [ "$RESET_OPTION_MENU" == "6" ];then
	echo "Entered Main Menu Option is $RESET_OPTION_MENU!"
	perform_truncate_table_adhoc;
	perform_menu_calling_auditstats_and_envreset;
	fi
	
	if [ "$RESET_OPTION_MENU" == "7" ];then
	echo "Entered Main Menu Option is $RESET_OPTION_MENU!"
	perform_audit_msdp01_batch;
	perform_retreive_rowkeys;
	perform_create_audit_tables_commands;
	perform_menu_calling_auditstats_and_envreset;
	fi
	
	if [ "$RESET_OPTION_MENU" == "8" ];then
	echo "Entered Main Menu Option is $RESET_OPTION_MENU!"
	perform_audit_msdp01_batch;
	perform_audit_msdp02_batch_status_details;
	perform_audit_msdp04_data_ingestion_status;
	perform_email_audit_stats;
	perform_menu_calling_auditstats_and_envreset;
	fi
	
	if [ "$RESET_OPTION_MENU" == "9" ];then
	echo "Entered Main Menu Option is $RESET_OPTION_MENU!"
	rm -f ${CURR_DIR}/*_COUNT_TEMP.txt;
	rm -f ${DB_TBL_STATS_FILE};
	perform_single_count_check;
	perform_menu_calling_auditstats_and_envreset;
	fi
	
	if [ "$RESET_OPTION_MENU" == "10" ];then
	rm -f ${CURR_DIR}/*_COUNT_TEMP.txt;
	rm -f ${DB_TBL_STATS_FILE};
	echo "Entered Main Menu Option is $RESET_OPTION_MENU!"
	perform_master_count_check;
	perform_menu_calling_auditstats_and_envreset;
	fi
		
	if [ "$RESET_OPTION_MENU" == "11" ];then
	echo "Entered Main Menu Option is $RESET_OPTION_MENU!"
	perform_menu_calling;
	fi
	
	if ([ "$RESET_OPTION_MENU" != "1"  ] || [ "$RESET_OPTION_MENU" != "2"  ] || [ "$RESET_OPTION_MENU" != "3"  ] \
	 || [ "$RESET_OPTION_MENU" != "4"  ] || [ "$RESET_OPTION_MENU" != "5"  ] || [ "$RESET_OPTION_MENU" != "6"  ] \
	 || [ "$RESET_OPTION_MENU" != "7"  ] || [ "$RESET_OPTION_MENU" != "8"  ] || [ "$RESET_OPTION_MENU" != "9"  ] \
	 || [ "$RESET_OPTION_MENU" != "10" ] || [ "$RESET_OPTION_MENU" != "11" ] || [ "x$RESET_OPTION_MENU" == "x" ]);then
	clear;
	echo ""
#	echo "INVALID OPTION ENTERED IN MAIN MENU ($RESET_OPTION_MENU)-- Please Enter the Appropriate Option Specified !! "
	perform_menu_calling_auditstats_and_envreset;
	fi
}

#----------------------------------------------------------------
# FUNCTION CALLING : Reset File Watchers
#----------------------------------------------------------------
function perform_reset_file_watchers
{
LOGGER_INPUT="Reset File Watchers ";

perform_user_activity_log ${LOGGER_INPUT};

    hadoop fs -rm -r -f /project/scac/$ENV_LC/filewatcher/$FULL_SRC_LC/*;
	hadoop fs -mkdir /project/scac/$ENV_LC/filewatcher/$FULL_SRC_LC/${SRC_MODULE_UC}_${DSC_FW_STATUS};
}
#----------------------------------------------------------------
# FUNCTION CALLING : Retreive Audit 01-Batch Table
#----------------------------------------------------------------
function perform_audit_msdp01_batch
{
LOGGER_INPUT="Retreive Audit information for 01-Batch Table ";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	MSDP01_BATCH_QUERY_LINE1=" SELECT $MSDP01_FIELDS FROM scac_${ENV_LC}_ops_db.msdp01_batch ";
	MSDP01_BATCH_QUERY_LINE2=" WHERE applicationname = '${SRC_MODULE_UC}' ";
	MSDP01_BATCH_QUERY_LINE3=" AND loadaction = '${PROCESS_TYPE_UC}' ";
	MSDP01_BATCH_QUERY=" $MSDP01_BATCH_QUERY_LINE1 $MSDP01_BATCH_QUERY_LINE2 $MSDP01_BATCH_QUERY_LINE3";
	
	echo $COS_FILLER2
	echo $MSDP01_BATCH_QUERY;
	echo $COS_FILLER2
	
	MSDP01_BATCH_QUERY_RESULT=`hive -e " ${MSDP01_BATCH_QUERY}"| sed -e 's/ //g' | sed -e 's/\t/,/g'`;
	MSDP01_BATCH_PROCESSDATECODE=`echo $MSDP01_BATCH_QUERY_RESULT | awk 'BEGIN{FS=","} {print $4}'`;
	MSDP01_BATCH_STATUS=`echo $MSDP01_BATCH_QUERY_RESULT | awk 'BEGIN{FS=","} {print $5}'`;
		
	rm -f $CURR_DIR/AUDIT_STATS_msdp01_${ENV_LC}_${FULL_SRC_LC}.csv;
	echo $MSDP01_FIELDS >> $CURR_DIR/AUDIT_STATS_msdp01_${ENV_LC}_${FULL_SRC_LC}.csv
	echo $MSDP01_BATCH_QUERY_RESULT  >> $CURR_DIR/AUDIT_STATS_msdp01_${ENV_LC}_${FULL_SRC_LC}.csv
	#echo $MSDP01_BATCH_QUERY_RESULT | sed -e 's/ //g' | sed -e 's/\t/,/g' >> $CURR_DIR/AUDIT_STATS_msdp01_${ENV_LC}_${FULL_SRC_LC}.csv
}
#----------------------------------------------------------------
# FUNCTION CALLING : Retreive ROW KEYS
#----------------------------------------------------------------
function perform_retreive_rowkeys
{
LOGGER_INPUT="Retreive ROW KEYS to Reset Audit Table ";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	rm -f $CURR_DIR/msdp04_data_ingestion_status_${ENV_LC}_${FULL_SRC_LC}.txt;

	MSDP04_BATCH_RKEY_QUERY_LINE1=" SELECT distinct(rowkey) FROM scac_${ENV_LC}_ops_db.msdp04_data_ingestion_status " 
	MSDP04_BATCH_RKEY_QUERY_LINE2=" WHERE applicationname='$SRC_MODULE_UC' "
	MSDP04_BATCH_RKEY_QUERY_LINE3=" AND processdatecode='$MSDP01_BATCH_PROCESSDATECODE' "
	#MSDP04_BATCH_RKEY_QUERY_LINE4=" AND loadaction='EXTRACT' "
	#MSDP04_BATCH_RKEY_QUERY=" $MSDP04_BATCH_RKEY_QUERY_LINE1 $MSDP04_BATCH_RKEY_QUERY_LINE2 $MSDP04_BATCH_RKEY_QUERY_LINE3 $MSDP04_BATCH_RKEY_QUERY_LINE4 "
	MSDP04_BATCH_RKEY_QUERY=" $MSDP04_BATCH_RKEY_QUERY_LINE1 $MSDP04_BATCH_RKEY_QUERY_LINE2 $MSDP04_BATCH_RKEY_QUERY_LINE3 "
	
	echo $COS_FILLER2
	echo $MSDP04_BATCH_RKEY_QUERY;
	echo $COS_FILLER2
	
	hive -e "$MSDP04_BATCH_RKEY_QUERY" | sed -e 's/ //g' | sed -e 's/\t/,/g' >>  $CURR_DIR/msdp04_data_ingestion_status_${ENV_LC}_${FULL_SRC_LC}.txt;
}
#----------------------------------------------------------------
# FUNCTION CALLING : Reset Audit Tables
#----------------------------------------------------------------
function perform_reset_audit_tables
{
LOGGER_INPUT="Reset Audit Table ";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	echo "put 'scac_$ENV_LC:msdp01_batch','"${SRC_MODULE_UC}_${PROCESS_TYPE_UC}"','general:Status','COMPLETED'" | hbase shell;
#	echo "put 'scac_$ENV_LC:msdp01_batch','"${SRC_MODULE_UC}_${PROCESS_TYPE_UC}"','general:ProcessDateCode',''" | hbase shell;
	echo $COS_FILLER2
	
	IFS=''
	while read line
	do
		MSDP04_MATCH_QUERY_DISTINCT_KEY=`echo $line | awk 'BEGIN{FS=","} {print $1}'`; 
		echo "deleteall 'scac_$ENV_LC:msdp04_data_ingestion_status','"$MSDP04_MATCH_QUERY_DISTINCT_KEY"'" | hbase shell;
		echo $COS_FILLER2;
	done < $CURR_DIR/msdp04_data_ingestion_status_${ENV_LC}_${FULL_SRC_LC}.txt
}
#----------------------------------------------------------------
# FUNCTION CALLING : Truncating T1 Tables
#----------------------------------------------------------------
function perform_truncate_t1
{	
LOGGER_INPUT="Truncating T1 Tables ";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	rm -f $CURR_DIR/trunc_t1_${ENV_LC}_${FULL_SRC_LC}.txt;
	
	hive -e "use ${FULL_SRC_LC}_${ENV_LC}_tz_scac_db; show tables like '*t1' " >> $CURR_DIR/trunc_t1_${ENV_LC}_${FULL_SRC_LC}.txt;
	
	IFS=''
	while read line
	do
	   echo $COS_FILLER2;
	   TRUNC_T1_TBL=`echo $line | awk 'BEGIN{FS=","} {print $1}'`;
 	   hive -e "truncate table ${FULL_SRC_LC}_${ENV_LC}_tz_scac_db.$TRUNC_T1_TBL";
	   echo "hive -e "truncate table ${FULL_SRC_LC}_${ENV_LC}_tz_scac_db.$TRUNC_T1_TBL"";
	   echo $COS_FILLER2;
	done < $CURR_DIR/trunc_t1_${ENV_LC}_${FULL_SRC_LC}.txt;
}
#----------------------------------------------------------------
# FUNCTION CALLING : Truncating Post-Staging Tables
#----------------------------------------------------------------
function perform_truncate_post_staging
{
LOGGER_INPUT="Truncating Post-Staging Tables ";

perform_user_activity_log ${LOGGER_INPUT};

	rm -f $CURR_DIR/trunc_poststage_${ENV_LC}_${FULL_SRC_LC}.txt;
	
	hive -e "use ${FULL_SRC_LC}_${ENV_LC}_sz_scac_db; show tables like '*post*'" >> $CURR_DIR/trunc_poststage_${ENV_LC}_${FULL_SRC_LC}.txt;
	
	IFS=''
	while read line
	do
	   echo $COS_FILLER2;
	   TRUNC_POST_TBL=`echo $line | awk 'BEGIN{FS=","} {print $1}'`;
 	   hive -e "truncate table ${FULL_SRC_LC}_${ENV_LC}_sz_scac_db.$TRUNC_POST_TBL";
	   echo "hive -e "truncate table ${FULL_SRC_LC}_${ENV_LC}_sz_scac_db.$TRUNC_POST_TBL"";
	   echo $COS_FILLER2;
	done < $CURR_DIR/trunc_poststage_${ENV_LC}_${FULL_SRC_LC}.txt;
}
#----------------------------------------------------------------
# FUNCTION CALLING : Truncating Pre-Staging Tables
#----------------------------------------------------------------
function perform_truncate_pre_staging
{
LOGGER_INPUT="Truncating Pre-Staging Tables ";

perform_user_activity_log ${LOGGER_INPUT};

	rm -f $CURR_DIR/trunc_prestage_${ENV_LC}_${FULL_SRC_LC}.txt;
	
	hive -e "use ${FULL_SRC_LC}_${ENV_LC}_sz_scac_db; show tables like '*pre*'" >> $CURR_DIR/trunc_prestage_${ENV_LC}_${FULL_SRC_LC}.txt;
	
	IFS=''
	while read line
	do
	   echo $COS_FILLER2;
	   TRUNC_PRE_TBL=`echo $line | awk 'BEGIN{FS=","} {print $1}'`;
	   hive -e "truncate table ${FULL_SRC_LC}_${ENV_LC}_sz_scac_db.$TRUNC_PRE_TBL";
	   echo "hive -e "truncate table ${FULL_SRC_LC}_${ENV_LC}_sz_scac_db.$TRUNC_PRE_TBL""
	   echo $COS_FILLER2;
	done < $CURR_DIR/trunc_prestage_${ENV_LC}_${FULL_SRC_LC}.txt;
}
#----------------------------------------------------------------
# FUNCTION CALLING : Truncating USER Entered Tables
#----------------------------------------------------------------
function perform_truncate_table_adhoc
{
LOGGER_INPUT="Truncating USER Entered Table(s) : $TRUNC_DB_ADHOC.$TRUNC_TBL_ADHOC ";

perform_user_activity_log ${LOGGER_INPUT};

	echo -n "Enter the Database Name of the Table to be Truncated : " 
	read TRUNC_DB_ADHOC;
	echo -n "Enter the Table Name to be Truncated                 : " 
	read TRUNC_TBL_ADHOC;

	echo "User Entered Database Name : $TRUNC_DB_ADHOC ";
	echo "User Entered Table    Name : $TRUNC_TBL_ADHOC";
	echo $COS_FILLER1;
	
	if ([ "x$TRUNC_DB_ADHOC" != "x" ] && [ "x$TRUNC_TBL_ADHOC" != "x" ]) 
	then
		echo "Truncating Table $TRUNC_DB_ADHOC.$TRUNC_TBL_ADHOC :"
		echo $COS_FILLER2;
		hive -e "truncate table $TRUNC_DB_ADHOC.$TRUNC_TBL_ADHOC";
		echo $COS_FILLER2;
	else
		echo $COS_FILLER1;
		echo "Please Enter Valid Database Name and Table Name in order to Truncate "
		echo "User Entered Database Name : $TRUNC_DB_ADHOC ";
		echo "User Entered Table    Name : $TRUNC_TBL_ADHOC";
		echo $COS_FILLER1;
	fi
}
#-----------------------------------------------------------------
# FUNCTION CALLING : Create Audit Tables Commands For Manual Entry
#-----------------------------------------------------------------
function perform_create_audit_tables_commands
{
LOGGER_INPUT="Create Audit Tables Commands For Manual Entry";

perform_user_activity_log ${LOGGER_INPUT};

	rm -f $CURR_DIR/manl_ntry_cmnds_${ENV_LC}_${FULL_SRC_LC}.txt;

	clear;
	echo "put 'scac_$ENV_LC:msdp01_batch','"$SRC_MODULE_UC_${PROCESS_TYPE_UC}"','general:Status','COMPLETED'" >> $CURR_DIR/manl_ntry_cmnds_${ENV_LC}_${FULL_SRC_LC}.txt;
	IFS=''
	while read line
	do
		MSDP04_MATCH_QUERY=`echo $line | awk 'BEGIN{FS=","} {print $1}'`; 
		echo "deleteall 'scac_${ENV_LC}:msdp04_data_ingestion_status','"$MSDP04_MATCH_QUERY"'" >> $CURR_DIR/manl_ntry_cmnds_${ENV_LC}_${FULL_SRC_LC}.txt;
	done < $CURR_DIR/msdp04_data_ingestion_status_${ENV_LC}_${FULL_SRC_LC}.txt
	
	echo $COS_FILLER1;
	echo $COS_FILLER2;
	echo " The Commands for Manual Entries are Extracted to File $CURR_DIR/msdp04_data_ingestion_status_${ENV_LC}_${FULL_SRC_LC}.txt ";
	echo $COS_FILLER2;
	echo $COS_FILLER1;
}
#----------------------------------------------------------------
# FUNCTION CALLING : Retreive Audit 02-Batch Table
#----------------------------------------------------------------
function perform_audit_msdp02_batch_status_details
{
LOGGER_INPUT="Retreive Audit information from 02-Batch Table ";

perform_user_activity_log ${LOGGER_INPUT};

	clear;

	MSDP02_BATCH_QUERY_LINE1=" SELECT $MSDP02_FIELDS FROM scac_${ENV_LC}_ops_db.msdp02_batch_status_details ";
	MSDP02_BATCH_QUERY_LINE2=" WHERE applicationname = '$SRC_MODULE_UC' ";
	MSDP02_BATCH_QUERY_LINE3=" AND processdatecode='$MSDP01_BATCH_PROCESSDATECODE' ";
	MSDP02_BATCH_QUERY=" $MSDP02_BATCH_QUERY_LINE1 $MSDP02_BATCH_QUERY_LINE2 $MSDP02_BATCH_QUERY_LINE3 ";
	
	echo $COS_FILLER2;
	echo $MSDP02_BATCH_QUERY;
	echo $COS_FILLER2;
	
	rm -f $CURR_DIR/AUDIT_STATS_msdp02_${ENV_LC}_${FULL_SRC_LC}.csv;
	echo $MSDP02_FIELDS >> $CURR_DIR/AUDIT_STATS_msdp02_${ENV_LC}_${FULL_SRC_LC}.csv;
	
	hive -e " $MSDP02_BATCH_QUERY"| sed -e 's/ //g' | sed -e 's/\t/,/g' >> $CURR_DIR/AUDIT_STATS_msdp02_${ENV_LC}_${FULL_SRC_LC}.csv;
}
#----------------------------------------------------------------
# FUNCTION CALLING : Retreive Audit 04-Batch Table
#----------------------------------------------------------------
function perform_audit_msdp04_data_ingestion_status
{
LOGGER_INPUT="Retreive Audit information from 04-Batch Table ";

perform_user_activity_log ${LOGGER_INPUT};

	clear;
	MSDP04_BATCH_QUERY_LINE1=" SELECT $MSDP04_FIELDS FROM scac_${ENV_LC}_ops_db.msdp04_data_ingestion_status " 
	MSDP04_BATCH_QUERY_LINE2=" WHERE applicationname='$SRC_MODULE_UC' "
	MSDP04_BATCH_QUERY_LINE3=" AND processdatecode='$MSDP01_BATCH_PROCESSDATECODE' "
	#MSDP04_BATCH_QUERY_LINE4=" AND loadaction = '$PROCESS_TYPE_UC' "
	#MSDP04_BATCH_QUERY=" $MSDP04_BATCH_QUERY_LINE1 $MSDP04_BATCH_QUERY_LINE2 $MSDP04_BATCH_QUERY_LINE3 $MSDP04_BATCH_QUERY_LINE4 "
	MSDP04_BATCH_QUERY=" $MSDP04_BATCH_QUERY_LINE1 $MSDP04_BATCH_QUERY_LINE2 $MSDP04_BATCH_QUERY_LINE3 "
	
	echo $COS_FILLER2;
	echo $MSDP04_BATCH_QUERY;
	echo $COS_FILLER2;
	
	rm -f $CURR_DIR/AUDIT_STATS_msdp04_${ENV_LC}_${FULL_SRC_LC}.csv;
	echo $MSDP04_FIELDS >> $CURR_DIR/AUDIT_STATS_msdp04_${ENV_LC}_${FULL_SRC_LC}.csv;
	
	hive -e " $MSDP04_BATCH_QUERY"| sed -e 's/ //g' | sed -e 's/\t/,/g' >> $CURR_DIR/AUDIT_STATS_msdp04_${ENV_LC}_${FULL_SRC_LC}.csv;
}
#----------------------------------------------------------------
# FUNCTION CALLING : Email Audit Statistics  
#----------------------------------------------------------------
function perform_email_audit_stats
{
LOGGER_INPUT="AUDIT EMAIL SENT TO $TO_EMAIL ";

perform_user_activity_log ${LOGGER_INPUT};

clear;
EMAIL_SUBJECT="SCAC ${ENV_UC} AUDIT STATS OF ${FULL_SRC_LC} FOR Processdatecode ${MSDP01_BATCH_PROCESSDATECODE}"
EMAIL_BODY="Refer the Email Attachment"
EMAIL_ATACHMENT=$CURR_DIR/AUDIT_STATS_*_${ENV_LC}_${FULL_SRC_LC}.csv

FILE_ATTACH1="AUDIT_STATS_msdp01_${ENV_LC}_${FULL_SRC_LC}.csv";
FILE_ATTACH2="AUDIT_STATS_msdp02_${ENV_LC}_${FULL_SRC_LC}.csv";
FILE_ATTACH3="AUDIT_STATS_msdp04_${ENV_LC}_${FULL_SRC_LC}.csv";

TEMPFILE=TEMP_AUDIT_STATS_msdp_${ENV_LC}_${FULL_SRC_LC}.txt

rm -f $CURR_DIR/${TEMPFILE};

cat $CURR_DIR/${FILE_ATTACH1} >> $CURR_DIR/${TEMPFILE};
echo "" >> $CURR_DIR/${TEMPFILE};
cat $CURR_DIR/${FILE_ATTACH2} >> $CURR_DIR/${TEMPFILE};
echo "" >> $CURR_DIR/${TEMPFILE};
cat $CURR_DIR/${FILE_ATTACH3} >> $CURR_DIR/${TEMPFILE};
echo "" >> $CURR_DIR/${TEMPFILE};

echo "cat ${CURR_DIR}/${TEMPFILE} | mutt -s "${EMAIL_SUBJECT}" -a ${EMAIL_ATACHMENT}  -- ${TO_EMAIL}";

cat ${CURR_DIR}/${TEMPFILE} | mutt -s "${EMAIL_SUBJECT}" -a ${EMAIL_ATACHMENT}  -- ${TO_EMAIL};

#echo ${EMAIL_BODY} | mutt -s '${EMAIL_SUBJECT}' -a ${EMAIL_ATACHMENT}  -- $TO_EMAIL
}
#----------------------------------------------------------------
# FUNCTION CALLING : Enabling Kerberos
#----------------------------------------------------------------
function perform_enable_kerberos
{
LOGGER_INPUT="Attempting to Enable Kerberos for user $CURR_USER in the server $CURR_SERVER ";

perform_user_activity_log ${LOGGER_INPUT};

clear;
kinit -k -t /s/$CURR_USER/$CURR_USER.keytab $CURR_USER; RS_KINIT=`echo $?`;
if  [ $RS_KINIT -eq 0 ];then
	echo $COS_FILLER1;
	echo $COS_FILLER2;
	echo " SUCCESS *SUCCESS *  Enabling Kerberos Successful SUCCESS *SUCCESS ";
	echo $COS_FILLER2;
	echo $COS_FILLER1;
else
	echo $COS_FILLER1;
	echo $COS_FILLER2;
	echo " ERROR * ERROR * ERROR *  Enabling Kerberos Failed  ERROR * ERROR * ERROR *  ";
	echo " Error While executing : kinit -k -t /s/$CURR_USER/$CURR_USER.keytab $CURR_USER ";
	echo $COS_FILLER2;
	echo $COS_FILLER1;
	exit 1;
fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Checks for the current executor and decides
#                    to procced or terminate the execution
#----------------------------------------------------------------
function perform_user_check
{
LOGGER_INPUT="Attempting to execute the script $PROGNAME in the server $CURR_SERVER";

perform_user_activity_log ${LOGGER_INPUT};

clear;
if ([ "$CURR_SERVER" == "hpchdp2e" ] && [ "$CURR_USER" != "iapxheam" ]);then
	echo $COS_FILLER1;
	echo $COS_FILLER2;
	echo " ERROR * ERROR * ERROR * ERROR *-------------------------------------------------------------  ERROR * ERROR * ERROR * ERROR *  ";
	echo $COS_FILLER2;
	echo " User '$CURR_USER' IS NOT AUTHORISED TO EXECUTE THE SCRIPT '$PROGNAME' IN THE SERVER '$CURR_SERVER' ";
	echo $COS_FILLER2;
	echo " ERROR * ERROR * ERROR * ERROR *-------------------------------------------------------------  ERROR * ERROR * ERROR * ERROR *  ";
	echo $COS_FILLER2;
	echo $COS_FILLER1;
	exit 1;
fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Checks for the current executor and decides
#                    to procced or terminate the execution
#----------------------------------------------------------------
function perform_user_activity_log
{
echo $LOGGER_INPUT
VAR_INPUT=$LOGGER_INPUT;
VAR_DATE=`date`;
echo $COS_FILLER2   >> ${CURR_DIR}/${LOG_FILE_NAME};
echo $VAR_DATE "---->> " ${VAR_INPUT}  >> ${CURR_DIR}/${LOG_FILE_NAME};
echo $COS_FILLER2   >> ${CURR_DIR}/${LOG_FILE_NAME};
}

#----------------------------------------------------------------
# FUNCTION CALLING : Retreive Landing Table Count for all Tables
#----------------------------------------------------------------
function perform_landing_count_check
{
	hive -S --hiveconf hive.execution.engine=tez -e "
	SELECT 
	'${LZ_DB_TABLE}'
	,count(*) 
	FROM ${LZ_DB_TABLE}" >> ${CURR_DIR}/LZ_COUNT_TEMP.txt  & PID_LZ=$!
}
#----------------------------------------------------------------
# FUNCTION CALLING : Retreive Pre Staging Table Count for all Tables
#----------------------------------------------------------------
function perform_prestaging_count_check
{
	hive -S --hiveconf hive.execution.engine=tez -e "
	SELECT 
	'${PRE_DB_TABLE}'
	,count(*) 
	FROM ${PRE_DB_TABLE}" >> ${CURR_DIR}/PRE_COUNT_TEMP.txt  & PID_PRE=$!
}
#----------------------------------------------------------------
# FUNCTION CALLING : Retreive Post Staging Table Count for all Tables
#----------------------------------------------------------------
function perform_poststaging_count_check
{
	hive -S --hiveconf hive.execution.engine=tez -e "
	SELECT 
	'${POST_DB_TABLE}' 
	,count(*) 
	FROM ${POST_DB_TABLE}" >> ${CURR_DIR}/POST_COUNT_TEMP.txt & PID_POST=$!
}
#----------------------------------------------------------------
# FUNCTION CALLING : Retreive Pre T1 Table Count for all Tables
#----------------------------------------------------------------
function perform_t1_count_check
{
	hive -S --hiveconf hive.execution.engine=tez -e "
	SELECT 
	'${T1_DB_TABLE}'
	,count(*) 
	FROM ${T1_DB_TABLE}" >> ${CURR_DIR}/T1_COUNT_TEMP.txt   & PID_T1=$!
}
#----------------------------------------------------------------
# FUNCTION CALLING : Write Count(s) to File 
#----------------------------------------------------------------
function perform_write_count
{
	VAR_LZ=`sed -e 's/\t/|/g' ${CURR_DIR}/LZ_COUNT_TEMP.txt `;echo ${CONST_LZ}${VAR_LZ} >> ${DB_TBL_STATS_FILE};
	VAR_PRE=`sed -e 's/\t/|/g' ${CURR_DIR}/PRE_COUNT_TEMP.txt`;echo ${CONST_PRE}${VAR_PRE} >> ${DB_TBL_STATS_FILE} ;
	VAR_POST=`sed -e 's/\t/|/g' ${CURR_DIR}/POST_COUNT_TEMP.txt`;echo ${CONST_POST}${VAR_POST} >> ${DB_TBL_STATS_FILE};
	VAR_T1=`sed -e 's/\t/|/g' ${CURR_DIR}/T1_COUNT_TEMP.txt`;echo ${CONST_T1}${VAR_T1} >> ${DB_TBL_STATS_FILE};
}
#----------------------------------------------------------------
# FUNCTION CALLING : Verify PIDs 
#----------------------------------------------------------------
function perform_pid_check
{
	VAR_PID="$PID_LZ $PID_PRE $PID_POST $PID_T1";
	
	wait $PID_LZ $PID_PRE $PID_POST $PID_T1;
	echo "PIDS are $VAR_PID ";
}
#----------------------------------------------------------------
# FUNCTION CALLING : Retreive Count for all Tables
#----------------------------------------------------------------
function perform_master_count_check
{
IFS=''
	while read line
	do		
		rm -f ${CURR_DIR}/*_COUNT_TEMP.txt;
		
		LZ_DB_TABLE=`echo $line | awk 'BEGIN{FS="|"} {print $1}'`; 
		PRE_DB_TABLE=`echo $line | awk 'BEGIN{FS="|"} {print $2}'`;
		POST_DB_TABLE=`echo $line | awk 'BEGIN{FS="|"} {print $3}'`;
		T1_DB_TABLE=`echo $line | awk 'BEGIN{FS="|"} {print $4}'`;

		perform_landing_count_check;
		perform_prestaging_count_check;
		perform_poststaging_count_check;
		perform_t1_count_check;
		perform_pid_check;
		perform_write_count;

		rm -f ${CURR_DIR}/*_COUNT_TEMP.txt;
		
	done < ${DB_TBL_FEED_FILE}
	
	echo "Hi" | mailx -s "Count Validation Report" -a ${DB_TBL_STATS_FILE} ${FORD_USER_CDSID}@ford.com
}
#----------------------------------------------------------------
# FUNCTION CALLING : Retreive Count for Single Tables
#----------------------------------------------------------------
function perform_single_count_check
{
echo -en ${yellow} "Enter Landing Zone Database.Table Name        :" ${nc} 
read LZ_DB_TABLE 
if [ "x$LZ_DB_TABLE" == "x" ];then
   echo "Please enter valid Enter Landing Zone Database.Table Name  :"
   echo ""
   exit 1
fi

echo -en ${yellow}  "Enter Pre-Staging Zone Database.Table Name    :" ${nc} 
read PRE_DB_TABLE 
if [ "x$PRE_DB_TABLE" == "x" ];then
   echo "Please enter valid Enter Pre-Staging Zone Database.Table Name  :" 
   echo ""
   exit 1
fi

echo -en ${yellow} "Enter Post-Staging Zone Database.Table Name   :" ${nc} 
read POST_DB_TABLE 
if [ "x$POST_DB_TABLE" == "x" ];then
   echo "Please enter valid Enter Post-Staging Zone Database.Table Name  :"
   echo ""
   exit 1
fi

echo -en ${yellow} "Enter T1 Zone Database.Table Name             :" ${nc}  
read T1_DB_TABLE 
if [ "x$T1_DB_TABLE" == "x" ];then
   echo "Please enter valid Enter T1 Zone Database.Table Name  :"
   echo ""
   exit 1
fi

	rm -f ${CURR_DIR}/*_COUNT_TEMP.txt;
	
	perform_landing_count_check;
	perform_prestaging_count_check;
	perform_poststaging_count_check;
	perform_t1_count_check;
	perform_pid_check;
	perform_write_count;
	
	rm -f ${CURR_DIR}/*_COUNT_TEMP.txt;
	
	echo "Hi" | mailx -s "Count Validation Report" -a ${DB_TBL_STATS_FILE} ${FORD_USER_CDSID}@ford.com
}
function perform_scac_count_for_qc
{	
	###### Declare Varaibles
	myday="$(date +'%Y_%m_%d')"
	mytime="$(date +'%T')"
	date_time=$myday"_"$mytime
	db_count_pid="$$"
	testpath="/gpfs/ess1-scratch/user/$USER/auto"
	export red='\e[0;31m'
	export bred='\e[1;31m'
	export ured='\e[4;31m'
	export nc='\e[0m' 
	export yellow='\e[1;33m'
	export bblue='\033[1;34m'
	export bgreen='\033[1;32m'

	echo -e ${bgreen}"******************************* Script Execution Started ******************************"${nc} 
	echo
	###### Get the Schemas
	echo -en ${yellow}"Enter Source Schema(DSC) Name :"${nc} 
	read src_db_name
	echo
	echo -en ${yellow}"Enter Pre/Post Staging Schema(SCA-SZ) Name :"${nc} 
	read tgt_db_name
	echo
	echo -en ${yellow}"Enter the SCAC Landing Schema Name :"${nc} 
	read land_db_name
	echo
	echo -en ${yellow}"Enter the Application Name :"${nc} 
	read app_name
	echo

	###### Extract the respective table names in Hadoop DB schema
	awk '{print $1}' input  > $testpath"/src_tables.txt"
	awk '{print $2}' input  > $testpath"/pre_tables.txt"
	awk '{print $3}' input  > $testpath"/post_tables.txt"
	awk '{print $4}' input  > $testpath"/land_tables.txt"

	src_ps_list="ps"
	src_prlist[0]="1234567"
	tgt_ps_list="ps"
	tgt_prlist[0]="1234567"
	post_ps_list="ps"
	post_prlist[0]="1234567"
	land_ps_list="ps"
	land_prlist[0]="1234567"

	mytty=$(tty | sed -e "s:/dev/::")

	scr_cnt_file_list=$src_db_name"_*.cnt"
	scr_hql_file_list=$src_db_name"_*.hql"

	if [ -f $testpath"/header.txt" ]; then
	   rm $testpath"/header.txt"
	fi 

	################################################## source table count validation##############
	#echo $scr_hpc_pass | kinit
	src_count_file_name=$src_db_name"_"$date_time".csv"

	# check for old PID file is there and remove
	if [ -f $testpath"/pid_"$src_db_name".txt" ]; then
		rm $testpath"/pid_"$src_db_name".txt"
	fi
	rm $testpath"/"$scr_cnt_file_list
	rm $testpath"/"$scr_hql_file_list

	echo -e ${bgreen}"***     Invoking Count processes for each table in "$src_db_name" DB(Source) ......"${nc} 

	# Loop thru each of the tables in the input file (source file)
	i=0
	while read -r src_line || [[ -n "$src_line" ]]
	do
		src_table_name="$src_line"
		export src_cmd="select '"$src_table_name"',count(*) from "$src_db_name"."$src_table_name";"''
		echo $src_cmd > $testpath"/"$src_db_name"_"$src_table_name".hql"
		chmod 777 $testpath"/"$src_db_name"_"$src_table_name".hql"
		src_hql_full_path=$testpath"/"$src_db_name"_"$src_table_name".hql"
		hive -S -f $src_hql_full_path >> $testpath"/"$src_db_name"_"$src_table_name".cnt" & 
		src_last_pid=$!
		src_prlist[$i]=$src_last_pid
		echo $src_last_pid >> $testpath"/pid_"$src_db_name".txt"
		src_ps_list=$src_ps_list" -p "$src_last_pid
		i=$(expr $i + 1)
	done < $testpath"/src_tables.txt"
	############################Done
	################################################## for pre tables counts ##############
	tgt_cnt_file_list=$tgt_db_name"_*pre.cnt"
	tgt_hql_file_list=$tgt_db_name"_*pre.hql"
	#echo $scr_hpc_pass | kinit
	tgt_count_file_name=$tgt_db_name"_"$date_time"_pre.csv"
	if [ -f $testpath"/pid_"$tgt_db_name"_pre.txt" ]; then
		rm $testpath"/pid_"$tgt_db_name"_pre.txt"
	fi
	rm $testpath"/"$tgt_cnt_file_list
	rm $testpath"/"$tgt_hql_file_list

	echo -e ${bgreen}"***     Invoking Count processes for each table in "$tgt_db_name" DB(staging PRE tables) ......"${nc} 

	# Loop thru each of the tables in the pre_tables.txt file (source file)
	j=0

	while read -r tgt_line || [[ -n "$tgt_line" ]]
	do
		tgt_table_name="$tgt_line"
		export tgt_cmd="select '"$tgt_table_name"',count(*) from "$tgt_db_name"."$tgt_table_name";"''
		echo $tgt_cmd > $testpath"/"$tgt_db_name"_"$tgt_table_name".hql"
		chmod 777 $testpath"/"$tgt_db_name"_"$tgt_table_name".hql"
		tgt_hql_full_path=$testpath"/"$tgt_db_name"_"$tgt_table_name".hql"
		hive -S -f $tgt_hql_full_path >> $testpath"/"$tgt_db_name"_"$tgt_table_name".cnt" &
		tgt_last_pid=$!
		tgt_prlist[$j]=$tgt_last_pid
		echo $tgt_last_pid >> $testpath"/pid_"$tgt_db_name"_pre.txt"
		tgt_ps_list=$tgt_ps_list" -p "$tgt_last_pid
		j=$(expr $j + 1)
	done < pre_tables.txt

	################################################## for post tables counts ##############
	post_cnt_file_list=$tgt_db_name"_*_post.cnt"
	post_hql_file_list=$tgt_db_name"_*_post.hql"
	#echo $scr_hpc_pass | kinit
	post_count_file_name=$tgt_db_name"_"$date_time"_post.csv"
	if [ -f $testpath"/pid_"$tgt_db_name"_post.txt" ]; then
		rm $testpath"/pid_"$tgt_db_name"_post.txt"
	fi
	rm $testpath"/"$post_cnt_file_list
	rm $testpath"/"$post_hql_file_list

	echo -e ${bgreen}"***     Invoking Count processes for each table in "$tgt_db_name" DB(staging POST tables) ......"${nc} 

	# Loop thru each of the tables in the post_tables.txt file (source file)
	echo
	echo
	echo
	k=0
	while read -r post_line || [[ -n "$post_line" ]]
	do
		post_table_name="$post_line"
		export post_cmd="select '"$post_table_name"',count(*) from "$tgt_db_name"."$post_table_name";"''
		echo $post_cmd > $testpath"/"$tgt_db_name"_"$post_table_name".hql"
		chmod 777 $testpath"/"$tgt_db_name"_"$post_table_name".hql"
		post_hql_full_path=$testpath"/"$tgt_db_name"_"$post_table_name".hql"
		hive -S -f $post_hql_full_path >> $testpath"/"$tgt_db_name"_"$post_table_name".cnt" &
		post_last_pid=$!
		post_prlist[$k]=$post_last_pid
		echo $post_last_pid >> $testpath"/pid_"$tgt_db_name"_post.txt"
		post_ps_list=$post_ps_list" -p "$post_last_pid
		k=$(expr $k + 1)
	done < post_tables.txt
	#########################################################

	################################################## for SCAC landing tables counts  ##############
	land_cnt_file_list=$land_db_name"_*_land.cnt"
	land_hql_file_list=$land_db_name"_*_land.hql"
	#echo $scr_hpc_pass | kinit
	land_count_file_name=$land_db_name"_"$date_time"_land.csv"
	if [ -f $testpath"/pid_"$land_db_name"_land.txt" ]; then
		rm $testpath"/pid_"$land_db_name"_land.txt"
	fi
	rm $testpath"/"$land_cnt_file_list
	rm $testpath"/"$land_hql_file_list

	echo -e ${bgreen}"***     Invoking Count processes for each table in "$land_db_name" DB(landing zone tables) ......"${nc} 

	# Loop thru each of the tables in the land_tables.txt file (source file)
	echo
	echo
	echo
	a=0
	while read -r land_line || [[ -n "$land_line" ]]
	do
		land_table_name="$land_line"
		export land_cmd="select '"$land_table_name"',count(*) from "$land_db_name"."$land_table_name";"''
		echo $land_cmd > $testpath"/"$land_db_name"_"$land_table_name"_land.hql"
		chmod 777 $testpath"/"$land_db_name"_"$land_table_name"_land.hql"
		land_hql_full_path=$testpath"/"$land_db_name"_"$land_table_name"_land.hql"
		hive -S -f $land_hql_full_path >> $testpath"/"$land_db_name"_"$land_table_name"_land.cnt" &
		land_last_pid=$!
		land_prlist[$a]=$land_last_pid
		echo $land_last_pid >> $testpath"/pid_"$land_db_name"_land.txt"
		land_ps_list=$land_ps_list" -p "$land_last_pid
		a=$(expr $a + 1)
	done < land_tables.txt
	#########################################################

	echo
	echo -e ${yellow}"************** ALL JOBS ARE RUNNING PARALLELLY **************"${yellow}${prlist[*]}${nc}
	echo
	sleep 20

	echo -e ${bgreen}"############## Waiting for all the count processes to end"${nc}
	echo 
	timetaken=20
	# Checking for all background processes are completed 

	while [ $($src_ps_list | wc -l) -gt 1 ] || [ $($tgt_ps_list | wc -l) -gt 1 ] || [ $($post_ps_list | wc -l) -gt 1 ] || [ $($land_ps_list | wc -l) -gt 1 ] 
	do
	 #   echo -e ${yellow}"Waiting for processes to end.... "${bblue}"Jobs running for past "${bred}$timetaken${yellow}" Secs"${nc}
		echo -e ${bgreen}"Source Tables    ->There are "${bred}$i${bgreen}" processe(s) totally... Still "${bred}$(expr $($src_ps_list | wc -l) - 1)${bgreen}" Processe(s) are running"${nc}
		echo -e ${bgreen}"Pre Tables       ->There are "${bred}$i${bgreen}" processe(s) totally... Still "${bred}$(expr $($tgt_ps_list | wc -l) - 1)${bgreen}" Processe(s) are running"${nc}
		echo -e ${bgreen}"Post Tables      ->There are "${bred}$i${bgreen}" processe(s) totally... Still "${bred}$(expr $($post_ps_list | wc -l) - 1)${bgreen}" Processe(s) are running"${nc}
		echo -e ${bgreen}"Landing Tables   ->There are "${bred}$i${bgreen}" processe(s) totally... Still "${bred}$(expr $($land_ps_list | wc -l) - 1)${bgreen}" Processe(s) are running"${nc}
		echo 
		echo -e ${yellow}"Please wait........"${nc}
		echo 
		sleep 25
		timetaken=$(expr $timetaken + 25)
		done
	# Building a count file for each schema	
	cat $testpath"/"$scr_cnt_file_list >> $testpath"/"$src_db_name"_"$date_time"_cnt.txt"
	cat $testpath"/"$tgt_cnt_file_list >> $testpath"/"$tgt_db_name"_"$date_time"_pre_cnt.txt"
	cat $testpath"/"$post_cnt_file_list >> $testpath"/"$tgt_db_name"_"$date_time"_post_cnt.txt"
	cat $testpath"/"$land_cnt_file_list >> $testpath"/"$land_db_name"_"$date_time"_land_cnt.txt"

	# remove all the temaparory files 
	rm $scr_cnt_file_list
	rm $scr_hql_file_list
	rm $tgt_cnt_file_list
	rm $tgt_hql_file_list
	rm $post_cnt_file_list
	rm $post_hql_file_list
	rm $land_cnt_file_list
	rm $land_hql_file_list

	sort -t$'\t' -k1 -n $testpath"/"$src_db_name"_"$date_time"_cnt.txt" >> $testpath"/"$src_db_name"_"$date_time".cnt"
	sort -t$'\t' -k1 -n $testpath"/"$tgt_db_name"_"$date_time"_pre_cnt.txt" >> $testpath"/"$tgt_db_name"_"$date_time"_pre.cnt"
	sort -t$'\t' -k1 -n $testpath"/"$tgt_db_name"_"$date_time"_post_cnt.txt" >> $testpath"/"$tgt_db_name"_"$date_time"_post.cnt"
	sort -t$'\t' -k1 -n $testpath"/"$land_db_name"_"$date_time"_land_cnt.txt" >> $testpath"/"$land_db_name"_"$date_time"_land.cnt"

	rm $testpath"/"$src_db_name"_"$date_time"_cnt.txt"
	rm $testpath"/"$tgt_db_name"_"$date_time"_pre_cnt.txt"
	rm $testpath"/"$tgt_db_name"_"$date_time"_post_cnt.txt"
	rm $testpath"/"$land_db_name"_"$date_time"_land_cnt.txt"

	rm $testpath"/pid_"$src_db_name".txt"
	rm $testpath"/pid_"$tgt_db_name"_pre.txt"
	rm $testpath"/pid_"$tgt_db_name"_post.txt"
	rm $testpath"/pid_"$land_db_name"_land.txt"

	rm $testpath"/src_tables.txt"
	rm $testpath"/pre_tables.txt"
	rm $testpath"/post_tables.txt"
	rm $testpath"/land_tables.txt"
	echo -e ${bgreen}"*****************    All processes are completed to get counts from  "${nc} 
	echo -e ${bgreen}"*********************************************	"$src_db_name"" ${nc} 
	echo -e ${bgreen}"*********************************************	"$tgt_db_name"" ${nc} 
	echo -e ${bgreen}"*********************************************	"$land_db_name"" ${nc} 
	echo
	#echo -e ${yellow}"*****************    Report Generation is in Progress   ****************************"${nc} 
	src_count_file_name=$testpath"/"$src_db_name"_"$date_time".cnt"
	tgt_count_file_name=$testpath"/"$tgt_db_name"_"$date_time"_pre.cnt"
	post_count_file_name=$testpath"/"$tgt_db_name"_"$date_time"_post.cnt"
	land_count_file_name=$testpath"/"$land_db_name"_"$date_time"_land.cnt"

	# Building the consolidated report 

	print_line="SOURCE_TABLE_NAME,SOURCE_RECORD_COUNT,PRE_TABLE_NAME,PRE_TABLE_COUNT,POST_TABLE_NAME,POST_TABLE_COUNT,LAND_TABLE_NAME,LAND_TABLE_COUNT,SRC_to_PRE_Match,SRC_to_Post_Match,SRC_to_LAND_Match"
	echo -e $print_line >> $testpath"/header.txt"

	touch $testpath"/"$app_name"_count_report_"$date_time".csv"
	touch $testpath"/"$app_name"_temp_count_report_"$date_time".csv"
	touch $testpath"/"$app_name"_temp1_count_report_"$date_time".csv"
	chmod -R 777 $testpath

	paste $src_count_file_name $tgt_count_file_name $post_count_file_name $land_count_file_name | sed 's/\t/,/g' > $testpath"/"$app_name"_temp_count_report_"$date_time".csv"
	chmod -R 777 $testpath

	# Add the validation formula
	i=1
	while read -r line || [[ -n "$line" ]]
	do
		i=$(expr $i + 1)
		print_line=$line           ## for source count cols
		print_line=$print_line",=B"$i"=D"$i  ## add formula for source_stage_count
		print_line=$print_line",=B"$i"=F"$i  ## add formula for source_stage_count
		print_line=$print_line",=B"$i"=H"$i  ## add formula for source_land_count
		echo $print_line >> $testpath"/"$app_name"_temp1_count_report_"$date_time".csv"
	done < $testpath"/"$app_name"_temp_count_report_"$date_time".csv"

	cat $testpath"/header.txt" $testpath"/"$app_name"_temp1_count_report_"$date_time".csv" > $testpath"/"$app_name"_count_report_"$date_time".csv"
	echo -e ${bgreen}"********************    Report Generation Completed   ***********************************"${nc} 
	echo
	chmod -R 777 $testpath

	# remove all the temp reports 

	rm $testpath"/header.txt"
	rm $testpath"/"$app_name"_temp1_count_report_"$date_time".csv"
	rm $testpath"/"$app_name"_temp_count_report_"$date_time".csv"


	rm $testpath"/"$src_db_name"_"$date_time".cnt"
	rm $testpath"/"$tgt_db_name"_"$date_time"_pre.cnt"
	rm $testpath"/"$tgt_db_name"_"$date_time"_post.cnt"
	rm $testpath"/"$land_db_name"_"$date_time"_land.cnt"

	# Sending the final report in email
	echo -e ${yellow}"*****************  Sending the count validation report in an email  ********************"${nc}
	echo
	echo "Hi,
		 
	Please find the attached count validation report for "$app_name".
			
	Thanks,
	SCA-C QC Team."  | mailx -s $app_name" - Count Validation Report" -a $testpath"/"$app_name"_count_report_"$date_time".csv" $USER@ford.com

	#rm $testpath"/"$app_name"_count_report_"$date_time".csv"

	echo -e ${bgreen}"********************************************************************************************"${nc}
	echo -e ${bgreen}"*                                                                                          *"${nc}
	echo -e ${bgreen}"  Count validation Completed and Report file : "$app_name"_count_report_"$date_time".csv    "${nc}
	echo -e ${bgreen}"*                                                                                          *"${nc}
	echo -e ${bgreen}"********************************************************************************************"${nc}
}

#---------------------------------------------------------------
#	MAIN LOGIC
#---------------------------------------------------------------

if ([ "$PERSON_ROLE_UC" == "DEV" ]);then
	CURR_DIR=${DEPLYD_LOCATION_UNIX};
	cd ${CURR_DIR};
	rm -f ${CURR_DIR}/${LOG_FILE_NAME};

	perform_user_activity_log ${LOGGER_INPUT};
	perform_user_check;
	perform_enable_kerberos;
	perform_menu_calling;
fi

if ([ "$PERSON_ROLE_UC" == "QC" ]);then
	perform_scac_count_for_qc
fi
