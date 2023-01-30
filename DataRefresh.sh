#!/bin/sh
#--------------------------------------------------------------------------------------------------#
#THIS SCRIPT REFRESHES DATA FROM HIGHER ENVIRONMENT (PROD) TO LOWER ENVIRONMENTS IN 2 STEPS        #
#                                                                                                  #
#  1. Extracts the contents of HIVE table from HIGHER ENV,based on user input or paramter file,as  #
#     HDFS FILE and moves it to target Environment                                                 #
#                                                                                                  #
#  2. Load the HDFS file into HIVE table in the target environment,based on user input or paramter #
#     file                                                                                         #
#                                                                                                  #
#Author : HVEDHASA                                                                                 #
#--------------------------------------------------------------------------------------------------#
#                   M O D I F I C A T I O N    L O G                                               #
#--------------------------------------------------------------------------------------------------#
# AUTHOR              DESCRIPTION			                                    DATE               #
# -------------     --------------------------------------------------------    -----------------  #
# hvedhasa          intial version                                              5-sep-2017         #
#                                                                                                  #
#                                                                                                  #
#--------------------------------------------------------------------------------------------------#
echo -e "\033c\e[3J";

#---------------------------------------------------------------
#CONSTANTS Declaration
#---------------------------------------------------------------
PROGNAME=$(basename $0)
CURR_SERVER=`hostname -s`;
CURR_USER=`whoami`;
CURR_DIR=`pwd`
COS_FILLER_SPACES=" ";
COS_FILLER_85_EQUALS="====================================================================================";
COS_FILLER_85_MINUS="------------------------------------------------------------------------------------";

FIELD_DELIMITER="\u0001"
PGM_LOGGER_INPUT='PROGRAM STARTED';
LOG_DIR="/s/${CURR_USER}/loggers/";
export COLR_RED='\e[0;31m';
export COLR_BRT_RED='\e[1;31m';
export COLR_UNDERLINED_RED='\e[4;31m';
export COLR_BRT_YELLOW='\e[1;33m'
export COLR_BRT_BLUE='\033[1;34m';
export COLR_BRT_GREEN='\033[1;32m';
export COLR_END='\e[0m' ;

#----------------------------------------------------------------
# FUNCTION CALLING : Global Variable Declaration
#----------------------------------------------------------------
function perform_0000_globalvariable_declaration
{
clear;
	echo -en ${COLR_BRT_YELLOW}"Enter the Application Name with CCR (eg : 10331_paten / 10004_aot_vot_polk) : " ${COLR_END}
	read GLBVRB_APP1
	GLBVRB_APP=`echo $GLBVRB_APP1 | tr '[:upper:]' '[:lower:]'`;
	if [ "x$GLBVRB_APP1" == "x" ];then
	   echo "Please enter valid Application Name with CCR (eg : 10331_paten / 10004_aot_vot_polk) :!"
	   exit 1
	fi

echo $COS_FILLER_SPACES;
	echo -en ${COLR_BRT_YELLOW} "Enter the TARGET User (eg : IAPXTAM or IAPXHQAM) : " ${COLR_END}
	read GLBVRB_TARGET_USER1
	GLBVRB_TARGET_USER=`echo $GLBVRB_TARGET_USER1 | tr '[:upper:]' '[:lower:]'`;
	if [ "x$GLBVRB_TARGET_USER1" == "x" ];then
	   echo "Please enter valid TARGET User (eg : IAPXHTAM or IAPXHQAM or IAPXHEAM) and non-empty  :!"
	   exit 1
	else   
	   if ([ "$GLBVRB_TARGET_USER" == "iapxhtam" ] || [ "$GLBVRB_TARGET_USER" == "iapxhqam" ] || [ "$GLBVRB_TARGET_USER" == "iapxheam" ]);then
	   	   echo " Valid Target user ... hence proceeding"
		   perform_0000_cross_env_access_compatibility;
		   UNIX_DATA_REFRESH_DIR="/s/${GLBVRB_TARGET_USER}/datarefresh/";
	       HDFS_DATA_REFRESH_DIR="/user/${CURR_USER}/datarefresh/";	
		else
		   echo "Please enter valid TARGET User (eg : IAPXHTAM or IAPXHQAM or IAPXHEAM)  :!"
		   exit 1
	   fi
	fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Cross Environment Access Compatibility check
#----------------------------------------------------------------
function perform_0000_cross_env_access_compatibility
{
test -x /s/${GLBVRB_TARGET_USER}/datarefresh/ ; RS_TARGET_WRITE_CHECK=`echo $?`;echo ${RS_TARGET_WRITE_CHECK};
test -w /s/${GLBVRB_TARGET_USER}/datarefresh/ ; RS_TARGET_EXECT_CHECK=`echo $?`;echo ${RS_TARGET_EXECT_CHECK};

if ([ "$RS_TARGET_WRITE_CHECK" == "0" ] && [ "$RS_TARGET_EXECT_CHECK" == "0" ]) ; then
	echo -e ${COLR_BRT_GREEN}" = = => PERFECT COMPTABILITY ! ! ! - user  '${CURR_USER}' has necessary priviliges on  '/s/${GLBVRB_TARGET_USER}/datarefresh/' "${COLR_END}
	
	PGM_LOGGER_INPUT=" perform_0000_cross_env_access_compatibility --> PERFECT COMPTABILITY ! ! !  user  '${CURR_USER}' has necessary priviliges on  '/s/${GLBVRB_TARGET_USER}/datarefresh/'"
	PGM_LOGGER_MAIN_MENU_OPTION="None";
	PGM_LOGGER_SUB_MENU_OPTION="None";
	perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION}
	
else
	echo -e ${COLR_BRT_GREEN} " ERROR ! ! ! ERROR ! ! ! "${COLR_END}
	echo -e ${COLR_BRT_GREEN} " ERROR ! ! ! ERROR ! ! ! - user  '${CURR_USER}' DOESN'T have necessary priviliges on target '/s/${GLBVRB_TARGET_USER}/datarefresh/'  or the target doesn't exist "${COLR_END}
	echo -e ${COLR_BRT_GREEN} " ERROR ! ! ! ERROR ! ! ! "${COLR_END}
	PGM_LOGGER_INPUT=" perform_0000_cross_env_access_compatibility -->  ERROR ! ! ! - user  '${CURR_USER}' DOESN'T have necessary priviliges on target '/s/${GLBVRB_TARGET_USER}/datarefresh/'  or the target doesn't  ";
	PGM_LOGGER_MAIN_MENU_OPTION="None";
	PGM_LOGGER_SUB_MENU_OPTION="None";
	perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION}
	exit 1;
fi	
}
#----------------------------------------------------------------
# FUNCTION CALLING : Checks for the current executor and decides
#                    to procced or terminate the execution
#----------------------------------------------------------------
function perform_0000_user_permission_check
{
PGM_LOGGER_INPUT=" perform_0000_user_permission_check - Performing User Check"
PGM_LOGGER_MAIN_MENU_OPTION="None";
PGM_LOGGER_SUB_MENU_OPTION="None";
perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION};

if ([ "$CURR_SERVER" == "hpchdp2e" ] && [ "$CURR_USER" != "iapxheam" ]);then
	echo $COS_FILLER_85_EQUALS;
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} " ERROR * ERROR * ERROR * ERROR *-------------------------------------------------------------  ERROR * ERROR * ERROR * ERROR *  "${COLR_END}
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} " User '$CURR_USER' IS NOT AUTHORISED TO EXECUTE THE SCRIPT '$PROGNAME' IN THE SERVER '$CURR_SERVER' "${COLR_END}
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} " ERROR * ERROR * ERROR * ERROR *-------------------------------------------------------------  ERROR * ERROR * ERROR * ERROR *  "${COLR_END}
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_85_EQUALS;
	exit 1;
fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Exits the program on User's Request
#----------------------------------------------------------------
function perform_0000_exit_program
{
PGM_LOGGER_INPUT=" perform_0000_exit_program - Closing the Program"
PGM_LOGGER_MAIN_MENU_OPTION=${OPTION_MAIN_MENU};
PGM_LOGGER_SUB_MENU_OPTION="None";

perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION};

clear;
echo -e ${COLR_BRT_GREEN}$COS_FILLER_85_EQUALS ${COLR_END};
echo $COS_FILLER_SPACES;
echo -e ${COLR_BRT_GREEN} "  * * THANK YOU FOR USING THE SCA-C DATA REFRESH TOOL , SEE YA LATER !!!! * * * "${COLR_END}
echo $COS_FILLER_SPACES;
echo -e ${COLR_BRT_GREEN}$COS_FILLER_85_EQUALS ${COLR_END};
exit 0;
}
#----------------------------------------------------------------
# FUNCTION CALLING : Exits the program on User's Request
#----------------------------------------------------------------
function perform_0000_logger
{
echo -e ${COLR_BRT_GREEN} ${COS_FILLER_SPACES} ${COLR_END};
echo -e ${COLR_BRT_GREEN}${PGM_LOGGER_INPUT} ${COLR_END};
echo -e ${COLR_BRT_GREEN} ${COS_FILLER_SPACES} ${COLR_END};

LOGGER_MESSAGE=${PGM_LOGGER_INPUT};
LOGGER_MAIN_MENU_OPTION=${PGM_LOGGER_MAIN_MENU_OPTION};
LOGGER_SUB_MENU_OPTION=${PGM_LOGGER_SUB_MENU_OPTION};
LOGGER_DATE=`date`;

echo $COS_FILLER_SPACES   >> $LOG_DIR/${PROGNAME}_${GLBVRB_APP}_log.txt;
echo $LOGGER_DATE "---->  MAIN-OPTION:${LOGGER_MAIN_MENU_OPTION} SUB-MENU:${LOGGER_SUB_MENU_OPTION} - " ${LOGGER_MESSAGE}  >> $LOG_DIR/${PROGNAME}_${GLBVRB_APP}_log.txt;
echo $COS_FILLER_SPACES   >> $LOG_DIR/${PROGNAME}_${GLBVRB_APP}_log.txt;
}
#----------------------------------------------------------------
# FUNCTION CALLING : Enabling Kerberos
#----------------------------------------------------------------
function perform_0000_enable_kerberos
{
PGM_LOGGER_INPUT=" perform_0000_enable_kerberos - Performing Kerberos Authentication"
PGM_LOGGER_MAIN_MENU_OPTION="None";
PGM_LOGGER_SUB_MENU_OPTION="None";
perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION};

clear;
kinit -k -t /s/$CURR_USER/$CURR_USER.keytab $CURR_USER; RS_KINIT=`echo $?`;
if  [ $RS_KINIT -eq 0 ];then
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_SPACES;
	echo " SUCCESS *SUCCESS *  Enabling Kerberos Successful SUCCESS *SUCCESS ";
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_SPACES;
else
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_SPACES;
	echo " ERROR * ERROR * ERROR *  Enabling Kerberos Failed  ERROR * ERROR * ERROR *  ";
	echo " Error While executing : kinit -k -t /s/$CURR_USER/$CURR_USER.keytab $CURR_USER ";
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_SPACES;
	exit 1;
fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Invoking the Main Menu
#----------------------------------------------------------------
function perform_invoke_main_menu
{
	clear;
	
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_85_EQUALS;
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN}"  *  *  *       WELCOME   TO   SCA-C  DATA   REFRESH   MAIN-MENU         *  *  * " ${COLR_END};
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN}"                      Enter your Option ( 1 till 3 Only )"${COLR_END}
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_85_EQUALS;
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "	                   1.      DATA EXTRACTION"${COLR_END}
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "	                   2.      DATA LOADING"${COLR_END}
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "	                   3.      EXIT"${COLR_END}
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} " NOTE : any Option other than the above will result in repeating the MAIN Menu"${COLR_END}
	echo $COS_FILLER_85_MINUS;
	echo $COS_FILLER_SPACES;
	echo -en ${COLR_BRT_GREEN} " Enter your Option ( 1 till 3 Only) : " ${COLR_END}
	read OPTION_MAIN_MENU
	echo $COS_FILLER_SPACES;

	if [ "$OPTION_MAIN_MENU" == "1" ];then
	echo "Entered Main Menu Option is $OPTION_MAIN_MENU!"
	perform_inovke_extraction_menu;
	perform_invoke_main_menu;
	fi
	
	if [ "$OPTION_MAIN_MENU" == "2" ];then
	echo "Entered Main Menu Option is $OPTION_MAIN_MENU!"
	perform_inovke_load_menu;
	perform_invoke_main_menu;
	fi
	
	if [ "$OPTION_MAIN_MENU" == "3" ];then
	echo "Entered Main Menu Option is $OPTION_MAIN_MENU!"
    perform_0000_exit_program;
	fi
	
	if [ "$OPTION_MAIN_MENU" != "1" ] || [ "$OPTION_MAIN_MENU" != "2" ] \
	|| [ "$OPTION_MAIN_MENU" != "3" ] || [ "x$OPTION_MAIN_MENU" == "x" ] ;then
	clear;
	echo $COS_FILLER_SPACES;
#	echo "INVALID OPTION ENTERED IN MAIN MENU ($OPTION_MAIN_MENU)-- Please Enter the Appropriate Option Specified !! "
	perform_invoke_main_menu;
	fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Invoking the Extraction  Menu
#----------------------------------------------------------------
function perform_inovke_extraction_menu
{
	clear;
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "  *  *  * DATA-REFRESH  DATA EXTRACT MAIN-MENU         *  *  * " ${COLR_END}
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "   Enter your Option ( 1 till 3 Only )" ${COLR_END};
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "	  1.  DATA EXTRACTION FOR SINGLE TABLE < user Driven>" ${COLR_END}
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "	  2.  DATA EXTRACTION FOR MULTIPLE TABLE <file driven>" ${COLR_END}
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "	  3.  RETURN TO MAIN MENU" ${COLR_END}
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} " NOTE : any Option other than the above will result in repeating the Extraction Menu" ${COLR_END}
	echo $COS_FILLER_85_MINUS;
	echo $COS_FILLER_SPACES;
	echo -en ${COLR_BRT_GREEN} " Enter your Option ( 1 till 3 Only) : " ${COLR_END} 
	read OPTION_EXTRACTION_MENU
	echo $COS_FILLER_SPACES;
	
	if [ "$OPTION_EXTRACTION_MENU" == "1" ];then
	echo "Entered Main Menu Option is $OPTION_EXTRACTION_MENU!";
	perform_0000_extract_single_table;
    perform_inovke_extraction_menu;	
	fi
	
	if [ "$OPTION_EXTRACTION_MENU" == "2" ];then
	echo "Entered Main Menu Option is $OPTION_EXTRACTION_MENU!";
	perform_0000_extract_multiple_table;
    perform_inovke_extraction_menu;
	fi
	
	if [ "$OPTION_EXTRACTION_MENU" == "3" ];then
	echo "Entered Main Menu Option is $OPTION_EXTRACTION_MENU!"
    perform_invoke_main_menu;
	fi
	
	if [ "$OPTION_EXTRACTION_MENU" != "1" ] || [ "$OPTION_EXTRACTION_MENU" != "2" ] \
	|| [ "$OPTION_EXTRACTION_MENU" != "3" ] || [ "x$OPTION_EXTRACTION_MENU" == "x" ] ;then
	clear;
	echo $COS_FILLER_SPACES;
	perform_inovke_extraction_menu;
	fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Invoking the Extraction  Menu
#----------------------------------------------------------------
function perform_inovke_load_menu
{
	clear;
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "  *  *  * DATA-REFRESH  DATA LOAD MAIN-MENU         *  *  * " ${COLR_END} ;
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "   Enter your Option ( 1 till 3 Only )" ${COLR_END} ;
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "	  1.  DATA LOAD FOR SINGLE TABLE " ${COLR_END} 
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "	  2.  DATA LOAD FOR MULTIPLE TABLE <file driven>" ${COLR_END} 
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "	  3.  RETURN TO MAIN MENU" ${COLR_END} 
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} " NOTE : any Option other than the above will result in repeating the Extraction Menu" ${COLR_END} 
	echo $COS_FILLER_85_MINUS;
	echo $COS_FILLER_SPACES;
	echo -en ${COLR_BRT_GREEN} " Enter your Option ( 1 till 3 Only) : "${COLR_END} 
	read OPTION_LOAD_MENU
	echo $COS_FILLER_SPACES;
	
	if [ "$OPTION_LOAD_MENU" == "1" ];then
	echo "Entered Main Menu Option is $OPTION_LOAD_MENU!";
	perform_0000_load_single_table;
    perform_inovke_load_menu;	
	fi
	
	if [ "$OPTION_LOAD_MENU" == "2" ];then
	echo "Entered Main Menu Option is $OPTION_LOAD_MENU!";
	perform_0000_load_multiple_table;
    perform_inovke_load_menu;
	fi
	
	if [ "$OPTION_LOAD_MENU" == "3" ];then
	echo "Entered Main Menu Option is $OPTION_LOAD_MENU!"
    perform_invoke_main_menu;
	fi
	
	if [ "$OPTION_LOAD_MENU" != "1" ] || [ "$OPTION_LOAD_MENU" != "2" ] \
	|| [ "$OPTION_LOAD_MENU" != "3" ] || [ "x$OPTION_LOAD_MENU" == "x" ] ;then
	clear;
	echo $COS_FILLER_SPACES;
	perform_inovke_load_menu;
	fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : VARIABLE INITIALISATION - Table Extract
#----------------------------------------------------------------
function perform_0001_01_extract_variable_initialisation
{
	SKIP_LINE="N";
	SORC_DBS="";
	SORC_TBL="";
	ROW_SAMPLING_LIMIT="";
	SORC_CUST_SQL_CHOICE="";
	CUSTOM_SQL_DB="";
	CUSTOM_SQL_TABLE="";
	HIVE_SETTINGS="";
	SORC_CUST_SQL="";
}
#---------------------------------------------------------------
#FUNCTION CALLING : VARIABLE DECLARATION - Data Extraction
#---------------------------------------------------------------
function perform_0001_extract_variable_declaration
{
if [ "${OPTION_EXTRACTION_MENU}" == "1" ]; then
#-----------------------
	echo $COS_FILLER_SPACES;
	
	perform_0001_01_extract_variable_initialisation; 
	
	echo -en ${COLR_BRT_YELLOW} "Do you want to perform Data Refresh with Custom SQL ? : (Y/y -> yes ; otherwise -> no): "${COLR_END}  
	read SORC_CUST_SQL_CHOICE1;
	SORC_CUST_SQL_CHOICE=`echo $SORC_CUST_SQL_CHOICE1 | tr '[:lower:]' '[:upper:]'`;	
	
	if [ "${SORC_CUST_SQL_CHOICE}" == "Y" ] ; then 
		echo $COS_FILLER_SPACES;
		echo -en ${COLR_BRT_YELLOW} "Enter the Custom SQL for Data Extraction:" ${COLR_END}
		read SORC_CUST_SQL;
			if [ "x$SORC_CUST_SQL" == "x" ];then
			   clear;
			   echo "ERROR ! ERROR !Please Enter APPROPRIATE Custom SQL for Data Extraction !"
			   exit 1;
			fi
			
			echo $COS_FILLER_SPACES;
			echo -en ${COLR_BRT_YELLOW} "Enter MAIN Source DB for Extraction (eg: 10004_aot_vot_polk_qa_lz_db) : "${COLR_END} 
			read SORC_DBS1
			SORC_DBS="CUSTOMSQL_"`echo $SORC_DBS1 | tr '[:upper:]' '[:lower:]'`;
				if [ "x$SORC_DBS1" == "x" ];then
				   echo "Please enter valid MAIN Source DB for Extraction (eg: 10004_aot_vot_polk_qa_lz_db) !"
				   exit 1
				fi
			#-----------------------
			echo $COS_FILLER_SPACES;
			echo -en ${COLR_BRT_YELLOW} "Enter MAIN Source Table for Extraction (eg: GDF0R33_AOT_VOT_POLK_VIN) : "${COLR_END} 
			read SORC_TBL1
			SORC_TBL="CUSTOMSQL_"`echo $SORC_TBL1 | tr '[:upper:]' '[:lower:]'`;
				if [ "x$SORC_TBL1" == "x" ];then
				   echo "Please enter valid MAIN Source Table for Extraction (eg: GDF0R33_AOT_VOT_POLK_VIN) !"
				   exit 1
				fi	
	else
		echo $COS_FILLER_SPACES;
			echo -en ${COLR_BRT_YELLOW} "Enter Source DB for Extraction (eg: 10004_aot_vot_polk_qa_lz_db) : "${COLR_END} 
			read SORC_DBS1
			SORC_DBS=`echo $SORC_DBS1 | tr '[:upper:]' '[:lower:]'`;
			if [ "x$SORC_DBS1" == "x" ];then
			   echo "Please enter valid Source DB for Extraction (eg: 10004_aot_vot_polk_qa_lz_db) !"
			   exit 1
			fi
		echo $COS_FILLER_SPACES;		
			#-----------------------
			echo $COS_FILLER_SPACES;
			echo -en ${COLR_BRT_YELLOW} "Enter Source Table for Extraction (eg: GDF0R33_AOT_VOT_POLK_VIN) : "${COLR_END} 
			read SORC_TBL1
			SORC_TBL=`echo $SORC_TBL1 | tr '[:upper:]' '[:lower:]'`;
			if [ "x$SORC_TBL1" == "x" ];then
			   echo "Please enter valid Source Table for Extraction (eg: GDF0R33_AOT_VOT_POLK_VIN) !"
			   exit 1
			fi
	fi		
		
	echo $COS_FILLER_SPACES;		
		#-----------------------
		echo $COS_FILLER_SPACES;
		echo -en ${COLR_BRT_YELLOW} "Enter the No of rows to be Extracted (any Positive Non-Zero Integer) : "${COLR_END} 
		read ROW_SAMPLING_LIMIT
		if ! [[ "$ROW_SAMPLING_LIMIT" =~ ^[0-9]+$ ]];then
			ROW_SAMPLING_LIMIT="1000"
		else
			if [ "$ROW_SAMPLING_LIMIT" == "0" ]; then
				ROW_SAMPLING_LIMIT="1000"
			else
				echo "Yes a valid +ve Non-Zeoro integer :  $ROW_SAMPLING_LIMIT"
				ROW_SAMPLING_LIMIT=${ROW_SAMPLING_LIMIT};
			fi
		fi		
	#-----------------------
	echo $COS_FILLER_SPACES;
	echo -en ${COLR_BRT_YELLOW} "Enter Hive Settings <optional>: " ${COLR_END}
	read HIVE_SETTINGS;		
fi 
#-----------------------
echo $COS_FILLER_SPACES;
if [ "${OPTION_EXTRACTION_MENU}" == "2" ]; then
		echo -en ${COLR_BRT_YELLOW} "Enter the Absolute Input File Containing the Mutiple Extraction details (eg: /s/iapxhtam/input.txt) : " ${COLR_END}
		read MULT_EXT_DETAILS_FILE
		if [ "x$MULT_EXT_DETAILS_FILE" == "x" ];then
		   echo "Please enter valid Absolute Input File Containing the Extraction details (eg: /s/iapxhtam/input.txt) !"
		   exit 1
		else
			if [ -s "$MULT_EXT_DETAILS_FILE" ];then
				echo "";
			else
				echo " !!!! ERROR !!! ERROR  "
				echo " !!!! ERROR !!! ERROR - File '${MULT_EXT_DETAILS_FILE}' does not exist or is empty or not accessible by user '${CURR_USER}'"
				echo " !!!! ERROR !!! ERROR  "
				exit 1;
			fi
		fi
fi
}
#----------------------------------------------------------------------------------------------------------------------------
# FUNCTION CALLING :- Retreive Source Table Data into HDFS
#----------------------------------------------------------------------------------------------------------------------------
function perform_0002_extract_src_table_data_to_hdfs
{
HDFS_PATH="/user/${CURR_USER}/datarefresh/";

hadoop fs -rm -f -r ${HDFS_PATH}/${SORC_DBS}_${SORC_TBL};
EXTRACT_TBL_HQL_LINE1=" INSERT OVERWRITE DIRECTORY '${HDFS_PATH}/${SORC_DBS}_${SORC_TBL}' ROW FORMAT DELIMITED ";
EXTRACT_TBL_HQL_LINE2=" FIELDS TERMINATED BY '${FIELD_DELIMITER}' STORED AS TEXTFILE";

if [ ${SORC_CUST_SQL_CHOICE} == "Y" ];then
	EXTRACT_TBL_HQL_LINE3="${SORC_CUST_SQL}";
else	
	EXTRACT_TBL_HQL_LINE3=" SELECT SRCTBL.* from ${SORC_DBS}.${SORC_TBL} SRCTBL"
fi
EXTRACT_TBL_HQL_LINE4=" LIMIT ${ROW_SAMPLING_LIMIT} ";

if 	[ "x${HIVE_SETTINGS}" != "x" ];then
	EXTRACT_TBL_HQL_LINE_ALL="${HIVE_SETTINGS};${EXTRACT_TBL_HQL_LINE1} ${EXTRACT_TBL_HQL_LINE2} ${EXTRACT_TBL_HQL_LINE3} ${EXTRACT_TBL_HQL_LINE4}"
else
	EXTRACT_TBL_HQL_LINE_ALL="${EXTRACT_TBL_HQL_LINE1} ${EXTRACT_TBL_HQL_LINE2} ${EXTRACT_TBL_HQL_LINE3} ${EXTRACT_TBL_HQL_LINE4}";
fi

#hive -e "${EXTRACT_TBL_HQL_LINE_ALL};";
beeline -e "${EXTRACT_TBL_HQL_LINE_ALL};";
RS_perform_0002_extract_src_table_data_to_hdfs=`echo $?`;

echo $EXTRACT_TBL_HQL_LINE_ALL;
}
#----------------------------------------------------------------------------------------------------------------------------
# FUNCTION CALLING :- Retreive Source HDFS Data to Unix File
#----------------------------------------------------------------------------------------------------------------------------
function perform_0003_extract_src_hdfs_data_to_unix
{
if [ "${RS_perform_0002_extract_src_table_data_to_hdfs}" == "0" ];then

	PGM_LOGGER_INPUT=" perform_0003_extract_src_hdfs_data_to_unix -->SUCCESS !!! -  Hive Extraction for ${SORC_DBS}.${SORC_TBL}";
	PGM_LOGGER_MAIN_MENU_OPTION=${OPTION_MAIN_MENU};
	PGM_LOGGER_SUB_MENU_OPTION=${OPTION_EXTRACTION_MENU};
	perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION}
	
	HDFS_PATH="/user/${CURR_USER}/datarefresh/";

	UNIX_PATH="/s/${CURR_USER}/datarefresh/";

	SAMPLED_SORC_FILE=SAMPLED_${SORC_DBS}_${SORC_TBL}.txt

	rm -r -f ${UNIX_PATH}/${SORC_DBS}_${SORC_TBL};

	mkdir -p ${UNIX_PATH}/${SORC_DBS}_${SORC_TBL} ;

	hadoop fs -get ${HDFS_PATH}/${SORC_DBS}_${SORC_TBL}/* ${UNIX_PATH}/${SORC_DBS}_${SORC_TBL}/;

	cat ${UNIX_PATH}/${SORC_DBS}_${SORC_TBL}/000000_*  > ${UNIX_PATH}/${SORC_DBS}_${SORC_TBL}/${SAMPLED_SORC_FILE} ;

	rm -f ${UNIX_PATH}/${SORC_DBS}_${SORC_TBL}/000000_* ;
	
else
	PGM_LOGGER_INPUT=" perform_0003_extract_src_hdfs_data_to_unix --> Skipping Data Extraction to Unix as Hive Extraction for ${SORC_DBS}.${SORC_TBL} was Unsuccessful"
	PGM_LOGGER_MAIN_MENU_OPTION=${OPTION_MAIN_MENU};
	PGM_LOGGER_SUB_MENU_OPTION=${OPTION_EXTRACTION_MENU};
	
	perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION}
fi
}
#----------------------------------------------------------------------------------------------------------------------------
# FUNCTION CALLING :- Move Data from One Unix Environment to another
#----------------------------------------------------------------------------------------------------------------------------
function perform_0004_extract_move_between_unix
{
if [ "${RS_perform_0002_extract_src_table_data_to_hdfs}" == "0" ];then

	PGM_LOGGER_MAIN_MENU_OPTION=${OPTION_MAIN_MENU};
	PGM_LOGGER_SUB_MENU_OPTION=${OPTION_EXTRACTION_MENU};
	PGM_LOGGER_INPUT=" perform_0004_extract_move_between_unix --> SUCCESS !!! -  Hive Extraction for ${SORC_DBS}.${SORC_TBL}";
	perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION};
	
	cp ${UNIX_PATH}/${SORC_DBS}_${SORC_TBL}/${SAMPLED_SORC_FILE}  ${UNIX_DATA_REFRESH_DIR}/ ;RS_COPY_BW_UNIX=`echo $?`
	
	chmod 777 ${UNIX_DATA_REFRESH_DIR}/${SAMPLED_SORC_FILE} ;RS_COPY_BW_UNIX_SET_PERMSN=`echo $?`

	if ([ "$RS_COPY_BW_UNIX" == "0" ] && [ "$RS_COPY_BW_UNIX_SET_PERMSN" == "0" ]);then
		rm -f ${UNIX_PATH}/${SORC_DBS}_${SORC_TBL}/${SAMPLED_SORC_FILE};
	else
		echo "RC PURGE - $RS_COPY_BW_UNIX_SET_PERMSN"
		echo "RC COPY  - $RS_COPY_BW_UNIX"
	fi
	
else
	PGM_LOGGER_INPUT=" perform_0004_extract_move_between_unix --> Skipping Data Move between Unix as Hive Extraction for ${SORC_DBS}.${SORC_TBL} was Unsuccessful"
	PGM_LOGGER_MAIN_MENU_OPTION=${OPTION_MAIN_MENU};
	PGM_LOGGER_SUB_MENU_OPTION=${OPTION_EXTRACTION_MENU};
	perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION}
fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Main Flow for Single Table extraction
#----------------------------------------------------------------
function perform_0000_extract_single_table
{
	perform_0001_extract_variable_declaration;
	perform_0002_extract_src_table_data_to_hdfs;
	perform_0003_extract_src_hdfs_data_to_unix;
	perform_0004_extract_move_between_unix;
}
#----------------------------------------------------------------
# FUNCTION CALLING : Main Flow for Multiple Table extraction
#----------------------------------------------------------------
function perform_0000_extract_multiple_table
{
perform_0001_extract_variable_declaration;

rm -f ${MULT_EXT_DETAILS_FILE}_TEMP;

sed -e "1d" ${MULT_EXT_DETAILS_FILE} > ${MULT_EXT_DETAILS_FILE}_TEMP

if [ -s "${MULT_EXT_DETAILS_FILE}_TEMP" ];then
	echo "";
else
	PGM_LOGGER_INPUT=" File '${MULT_EXT_DETAILS_FILE}' contains only HEADER [or] the HEADER line is replaced by DETAIL line ";
	PGM_LOGGER_MAIN_MENU_OPTION=${OPTION_MAIN_MENU};
	PGM_LOGGER_SUB_MENU_OPTION=${OPTION_EXTRACTION_MENU};
	perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION};	
	echo " !!!! WARNING !!! WARNING  ";
	echo " !!!! WARNING !!! WARNING - ${PGM_LOGGER_INPUT}";
	echo " !!!! WARNING !!! WARNING  ";
fi

LINE_INDICATOR="1"; # Pointer for current Line Number
	IFS=''
	while read line
	do
	
		perform_0001_01_extract_variable_initialisation; 
		
		SORC_CUST_SQL_CHOICE=`echo $line | awk 'BEGIN{FS="|"} {print $4}' | tr '[:lower:]' '[:upper:]'` 

#======================= Check the Values from Parameter file/Input File		
		if [ ${SORC_CUST_SQL_CHOICE} == "Y" ];then 
		SORC_DBS=`echo $line | awk 'BEGIN{FS="|"} {print $5}'`;
			if [ "x${SORC_DBS}" == "x"   ];then 
				SKIP_LINE="Y" 
			else
				CUSTOM_SQL_DB=`echo "CUSTOMSQL_"${SORC_DBS} | tr '[:lower:]' '[:upper:]'`;
				SORC_DBS=${CUSTOM_SQL_DB};
			fi
		SORC_TBL=`echo $line | awk 'BEGIN{FS="|"} {print $6}'` ;
			if [ "x${SORC_TBL}" == "x"   ];then 
				SKIP_LINE="Y" 
			else
				CUSTOM_SQL_TBL=`echo "CUSTOMSQL_"${SORC_TBL} | tr '[:lower:]' '[:upper:]'`;
				SORC_TBL=${CUSTOM_SQL_TBL};
			fi
		SORC_CUST_SQL=`echo $line | awk 'BEGIN{FS="|"} {print $8}'`
		else 
			SORC_DBS=`echo $line | awk 'BEGIN{FS="|"} {print $1}'`  ;if [ "x$SORC_DBS" == "x"   ];then SKIP_LINE="Y" ;fi;
			
			SORC_TBL=`echo $line | awk 'BEGIN{FS="|"} {print $2}'`  ;if [ "x$SORC_TBL" == "x"   ];then SKIP_LINE="Y" ;fi;
		fi	

		ROW_SAMPLING_LIMIT=`echo $line | awk 'BEGIN{FS="|"} {print $3}'`;
			if ! [[ "$ROW_SAMPLING_LIMIT" =~ ^[0-9]+$ ]];then
				ROW_SAMPLING_LIMIT="1000"
			else
				if [ "$ROW_SAMPLING_LIMIT" == "0" ]; then
					ROW_SAMPLING_LIMIT="1000"
				else
					ROW_SAMPLING_LIMIT=${ROW_SAMPLING_LIMIT};
				fi
			fi

		HIVE_SETTINGS=`echo $line | awk 'BEGIN{FS="|"} {print $7}'`;
	
#======================= Continues only if Validation is successful else the Param Entry is Skipped		
		if [ "$SKIP_LINE" == "Y" ];then
			perform_0008_extract_multiple_table_error;
		else
			perform_0002_extract_src_table_data_to_hdfs;
			perform_0003_extract_src_hdfs_data_to_unix;
			perform_0004_extract_move_between_unix;			
			if [ "${RS_perform_0002_extract_src_table_data_to_hdfs}" != "0" ];then
				perform_0008_extract_multiple_table_error;
			else
				perform_0008_extract_multiple_table_success;
			fi
		fi
	((LINE_INDICATOR+=1));
	done < ${MULT_EXT_DETAILS_FILE}_TEMP
}
#----------------------------------------------------------------
# FUNCTION CALLING : VARIABLE INITIALISATION - Table Load
#----------------------------------------------------------------
function perform_0001_01_load_variable_initialisation
{
	SKIP_LINE="N";
	FILENAME="";
	TARGT_STAGING_DB="";
	TEMP_TABLE="";
	PARTITION_FLAG="";
	TARGT_TBL_PART_COL="";
	TARGT_PRE_TABLE="";
	TARGT_TABLE="";
}
#----------------------------------------------------------------
# FUNCTION CALLING : VARIABLE DECLARATION - Table Load
#----------------------------------------------------------------
function perform_0001_load_variable_declaration
{
if [ "${OPTION_LOAD_MENU}" == "1" ]; then

	perform_0001_01_load_variable_initialisation; 

#-----------------------
		echo $COS_FILLER_SPACES;

		echo -en ${COLR_BRT_YELLOW}"Enter File to be loaded along with Path : "${COLR_END}
		read FILENAME
		if [ "x$FILENAME" == "x" ];then
		   echo "Please enter valid File to be loaded along with Path "
		   exit 1
		fi
echo $COS_FILLER_SPACES;	
#-----------------------
		echo $COS_FILLER_SPACES;
		echo -en ${COLR_BRT_YELLOW} "Enter Target DB for Loading : "${COLR_END}
		read TARGT_STAGING_DB1
		TARGT_STAGING_DB=`echo $TARGT_STAGING_DB1 | tr '[:upper:]' '[:lower:]'`;
		if [ "x$TARGT_STAGING_DB1" == "x" ];then
		   echo "Please enter valid Target DB for Loading (eg: 10004_aot_vot_polk_qa_lz_db) !"
		   exit 1
		fi
echo $COS_FILLER_SPACES;		
#-----------------------
		echo $COS_FILLER_SPACES;
		echo -en ${COLR_BRT_YELLOW} "Enter Temporary Table for Loading  :"${COLR_END}
		read TEMP_TABLE1
		TEMP_TABLE=`echo $TEMP_TABLE1 | tr '[:upper:]' '[:lower:]'`;
		if [ "x$TEMP_TABLE1" == "x" ];then
		   echo "Please enter valid Temporary Table for Loading (eg: GDF0R33_AOT_VOT_POLK_VIN_TEMP)  !"
		   exit 1
		fi
echo $COS_FILLER_SPACES;		
#-----------------------
		echo $COS_FILLER_SPACES;
		echo -en ${COLR_BRT_YELLOW} "Enter PreStaging table : "${COLR_END} 
		read TARGT_PRE_TABLE1
		TARGT_PRE_TABLE=`echo $TARGT_PRE_TABLE1 | tr '[:upper:]' '[:lower:]'`;
		if [ "x$TARGT_PRE_TABLE1" == "x" ];then
		   echo "Please enter valid PreStaging Table!"
		   exit 1
		fi
echo $COS_FILLER_SPACES;		
#-----------------------
		echo $COS_FILLER_SPACES;
		echo -en ${COLR_BRT_YELLOW} "Enter Target Table to load the data : "${COLR_END}
		read TARGT_TABLE1
		TARGT_TABLE=`echo $TARGT_TABLE1 | tr '[:upper:]' '[:lower:]'`;
		if [ "x$TARGT_TABLE" == "x" ];then
		   echo "Please enter valid Target Table to load the data  : !"
		   exit 1
		fi
echo $COS_FILLER_SPACES;	
#-----------------------
		echo $COS_FILLER_SPACES;
		echo -en ${COLR_BRT_YELLOW} "Is the Source partitioned ? : (Y/y - yes otherwise -> no) "${COLR_END}
		read PARTITION_FLAG;
		PARTITION_FLAG=`echo $PARTITION_FLAG | tr '[:lower:]' '[:upper:]'`;
		if [ "${PARTITION_FLAG}" == "Y" ];then
			echo $COS_FILLER_SPACES;
			echo -en ${COLR_BRT_YELLOW} "Enter Partition Column for the Target Table (eg: df0r33_lst_updt_s) : "${COLR_END} 
			read TARGT_TBL_PART_COL1
			TARGT_TBL_PART_COL=`echo $TARGT_TBL_PART_COL1 | tr '[:upper:]' '[:lower:]'`;
			if [ "x$TARGT_TBL_PART_COL1" == "x" ];then
			   echo "Please enter valid Partition Column for the Target Table (eg: df0r33_lst_updt_s)  !"
			   exit 1
			fi
		fi	
echo $COS_FILLER_SPACES;	
fi 
#-----------------------
echo $COS_FILLER_SPACES;
if [ "${OPTION_LOAD_MENU}" == "2" ]; then
		echo -en ${COLR_BRT_YELLOW} "Enter the Absolute Input File Containing the Mutiple Loading details (eg: /s/iapxhtam/input.txt) : "${COLR_END} 
		read MULT_LOD_DETAILS_FILE
		if [ "x$MULT_LOD_DETAILS_FILE" == "x" ];then
		   echo "Please enter valid Absolute Input File Containing the Loading details (eg: /s/iapxhtam/input.txt) !"
		   exit 1
		else
			if [ -s "$MULT_LOD_DETAILS_FILE" ];then
				echo "";
			else
				echo " !!!! ERROR !!! ERROR  "
				echo " !!!! ERROR !!! ERROR - File '${MULT_LOD_DETAILS_FILE}' does not exist or is empty or not accessible by user '${CURR_USER}'"
				echo " !!!! ERROR !!! ERROR  "
				exit 1;
			fi
		fi
fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : KILL and FILL the Temporary Table
#					 Create Target Lable like Prestaging
#----------------------------------------------------------------
function perform_0002_load_temp_table
{
	#RS_perform_0002_load_temp_table_flag="1";
	
	PGM_LOGGER_INPUT=" Entering  perform_0002_load_temp_table "
	PGM_LOGGER_MAIN_MENU_OPTION=${OPTION_MAIN_MENU};
	PGM_LOGGER_SUB_MENU_OPTION=${OPTION_LOAD_MENU};	
	perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION};

	hadoop fs -rm -r -f  /user/${CURR_USER}/${TARGT_STAGING_DB}_${TEMP_TABLE}/

	hadoop fs -mkdir /user/${CURR_USER}/${TARGT_STAGING_DB}_${TEMP_TABLE}/

	hadoop fs -put ${FILENAME}  /user/${CURR_USER}/${TARGT_STAGING_DB}_${TEMP_TABLE}/

	HQL_LOAD_TEMP_TABLE_1="TRUNCATE TABLE ${TARGT_STAGING_DB}.${TEMP_TABLE};LOAD DATA INPATH ";
	HQL_LOAD_TEMP_TABLE_2=" '/user/${CURR_USER}/${TARGT_STAGING_DB}_${TEMP_TABLE}/' ";
	HQL_LOAD_TEMP_TABLE_3=" INTO TABLE ${TARGT_STAGING_DB}.${TEMP_TABLE}; ";
	HQL_LOAD_TEMP_TABLE=" ${HQL_LOAD_TEMP_TABLE_1} ${HQL_LOAD_TEMP_TABLE_2} ${HQL_LOAD_TEMP_TABLE_3}"
	echo "${HQL_LOAD_TEMP_TABLE}"
	beeline -e " ${HQL_LOAD_TEMP_TABLE}";
	RS_perform_load_temp_table=`echo $?`;
	
	HQL_CREATE_TGT_1="CREATE TABLE IF NOT EXISTS ${TARGT_STAGING_DB}.${TARGT_TABLE}";
	HQL_CREATE_TGT_2=" LIKE ${TARGT_STAGING_DB}.${TARGT_PRE_TABLE}; ";
	HQL_CREATE_TGT=" ${HQL_CREATE_TGT_1} ${HQL_CREATE_TGT_2}"
	echo "${HQL_CREATE_TGT}"
	beeline -e " ${HQL_CREATE_TGT}";
	RS_perform_create_target=`echo $?`;
	
	#sets the Flag to 0 Only if above HQLs are successful
	
	if ([ ${RS_perform_load_temp_table} != "0" ] || [ ${RS_perform_create_target} != "0" ]); then
		RS_perform_0002_load_temp_table_flag="1"; #Failure
	else
		RS_perform_0002_load_temp_table_flag="0"; #Success
	fi
	}
#----------------------------------------------------------------
# FUNCTION CALLING : Load the target Table
#----------------------------------------------------------------
function perform_0004_load_target_table_from_temp
{
PGM_LOGGER_INPUT=" Entering  perform_0004_load_target_table_from_temp "
PGM_LOGGER_MAIN_MENU_OPTION=${OPTION_MAIN_MENU};
PGM_LOGGER_SUB_MENU_OPTION=${OPTION_LOAD_MENU};
perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION}
#RS_perform_0004_load_target_table_from_temp="1";


if [ "${RS_perform_0002_load_temp_table_flag}" == "0" ];then
	if [ ${PARTITION_FLAG} == "Y" ];then 

		
		HQL_LOAD_TGT_FROM_TEMP1=" INSERT OVERWRITE TABLE ${TARGT_STAGING_DB}.${TARGT_TABLE} PARTITION (${TARGT_TBL_PART_COL}) ";
		HQL_LOAD_TGT_FROM_TEMP2=" SELECT TEMPTBL.* FROM ${TARGT_STAGING_DB}.${TEMP_TABLE} TEMPTBL;";	
		HQL_LOAD_TGT_FROM_TEMP="${HQL_LOAD_TGT_FROM_TEMP1} ${HQL_LOAD_TGT_FROM_TEMP2} ";

		PGM_LOGGER_INPUT=" perform_0004_load_target_table_from_temp --> ${HQL_LOAD_TGT_FROM_TEMP} "
		PGM_LOGGER_MAIN_MENU_OPTION=${OPTION_MAIN_MENU};
		PGM_LOGGER_SUB_MENU_OPTION=${OPTION_LOAD_MENU};
		perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION}
		
		#hive -e " ${HQL_LOAD_TGT_FROM_TEMP}" 
		beeline -e " ${HQL_LOAD_TGT_FROM_TEMP}" 
		RS_perform_0004_load_target_table_from_temp=`echo $?`
	else
		PGM_LOGGER_INPUT=" perform_0004_load_target_table_from_temp -->INSERT OVERWRITE TABLE ${TARGT_STAGING_DB}.${TARGT_TABLE} SELECT * FROM ${TARGT_STAGING_DB}.${TEMP_TABLE}; "
		PGM_LOGGER_MAIN_MENU_OPTION=${OPTION_MAIN_MENU};
		PGM_LOGGER_SUB_MENU_OPTION=${OPTION_LOAD_MENU};		
		perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION}
		
		HQL_LOAD_TGT_FROM_TEMP_1=" INSERT OVERWRITE TABLE ${TARGT_STAGING_DB}.${TARGT_TABLE} ";
		HQL_LOAD_TGT_FROM_TEMP_2=" SELECT TEMPTBL.* FROM ${TARGT_STAGING_DB}.${TEMP_TABLE} TEMPTBL;";	
		HQL_LOAD_TGT_FROM_TEMP="${HQL_LOAD_TGT_FROM_TEMP_1} ${HQL_LOAD_TGT_FROM_TEMP_2} ";
		echo ${HQL_LOAD_TGT_FROM_TEMP};
		
		#hive -e " ${HQL_LOAD_TGT_FROM_TEMP}" 
		beeline -e " ${HQL_LOAD_TGT_FROM_TEMP}" 
		RS_perform_0004_load_target_table_from_temp=`echo $?`
	fi
else 
	PGM_LOGGER_INPUT=" perform_0004_load_target_table_from_temp -->SKIPPED !!! - As Loading Temp ${TARGT_STAGING_DB}.${TEMP_TABLE} failed "
	PGM_LOGGER_MAIN_MENU_OPTION=${OPTION_MAIN_MENU};
	PGM_LOGGER_SUB_MENU_OPTION=${OPTION_LOAD_MENU};
	perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION}
	RS_perform_0004_load_target_table_from_temp="1"
fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Main Flow for Single Table Load
#----------------------------------------------------------------
function perform_0000_load_single_table
{
	perform_0001_load_variable_declaration;	
	perform_0002_load_temp_table;
	perform_0004_load_target_table_from_temp;
}
#----------------------------------------------------------------
# FUNCTION CALLING : Main Flow for Multiple Table Load
#----------------------------------------------------------------
function perform_0000_load_multiple_table
{
clear;
perform_0001_load_variable_declaration;

#rm -f ${MULT_LOD_DETAILS_FILE}_TEMP
	
sed -e "1d" ${MULT_LOD_DETAILS_FILE} > ${MULT_LOD_DETAILS_FILE}_TEMP

#echo "000000000000000000000000000000000000000000000"
#cat ${MULT_LOD_DETAILS_FILE}_TEMP;
#echo "000000000000000000000000000000000000000000000"

if [ -s "${MULT_LOD_DETAILS_FILE}_TEMP" ];then
	echo "";
else
	PGM_LOGGER_INPUT=" File '${MULT_LOD_DETAILS_FILE}' contains only HEADER [or] the HEADER line is replaced by DETAIL line ";
	PGM_LOGGER_MAIN_MENU_OPTION=${OPTION_MAIN_MENU};
	PGM_LOGGER_SUB_MENU_OPTION=${OPTION_LOAD_MENU};
	perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION};	
	echo " !!!! WARNING !!! WARNING  ";
	echo " !!!! WARNING !!! WARNING - ${PGM_LOGGER_INPUT}";
	echo " !!!! WARNING !!! WARNING  ";
fi

LINE_INDICATOR="1";
	IFS=''
	while read line
	do
		perform_0001_01_load_variable_initialisation; 

		FILENAME=`echo $line | awk 'BEGIN{FS="|"} {print $1}'`;
			if [ "x${FILENAME}" == "x"   ];then 
				SKIP_LINE="Y" 
			fi		
		TARGT_STAGING_DB=`echo $line | awk 'BEGIN{FS="|"} {print $2}' | tr '[:lower:]' '[:upper:]'` 
			if [ "x${TARGT_STAGING_DB}" == "x"   ];then 
				SKIP_LINE="Y" 
			fi			
		TEMP_TABLE=`echo $line | awk 'BEGIN{FS="|"} {print $3}' | tr '[:lower:]' '[:upper:]'`
			if [ "x${TEMP_TABLE}" == "x"   ];then 
				SKIP_LINE="Y" 
			fi
		PARTITION_FLAG=`echo $line | awk 'BEGIN{FS="|"} {print $4}' | tr '[:lower:]' '[:upper:]'`;
			if [ "${PARTITION_FLAG}" == "Y" ];then
				TARGT_TBL_PART_COL=`echo $line | awk 'BEGIN{FS="|"} {print $5}' | tr '[:lower:]' '[:upper:]'` 
				if [ "x${TARGT_TBL_PART_COL}" == "x"   ];then 
					SKIP_LINE="Y"; 
				fi	
			fi
		TARGT_PRE_TABLE=`echo $line | awk 'BEGIN{FS="|"} {print $6}' | tr '[:lower:]' '[:upper:]'` 
			if [ "x${TARGT_PRE_TABLE}" == "x"   ];then 
				SKIP_LINE="Y" 
			fi	
		TARGT_TABLE=`echo $line | awk 'BEGIN{FS="|"} {print $7}' | tr '[:lower:]' '[:upper:]'` 
			if [ "x${TARGT_TABLE}" == "x"   ];then 
				SKIP_LINE="Y" 
			fi		
#======================= Continues only if Validation is successful else the Param Entry is Skipped				
		if [ "$SKIP_LINE" == "Y" ];then
			perform_0009_load_multiple_table_error;
		else
			perform_0002_load_temp_table;
			perform_0004_load_target_table_from_temp;
			if ([ "${RS_perform_0004_load_target_table_from_temp}" != "0" ] || [ "${RS_perform_0002_load_temp_table_flag}" != "0" ]);then
				perform_0009_load_multiple_table_error;
			else
				perform_0009_load_multiple_table_success;
			fi			
		fi
	((LINE_INDICATOR+=1));
	done < ${MULT_LOD_DETAILS_FILE}_TEMP
}
#----------------------------------------------------------------
# FUNCTION CALLING : Message to be displayed when mutliple extract 
#					 is IMPROPER due to the Extract Input File
#----------------------------------------------------------------
function perform_0008_extract_multiple_table_error
{
	PGM_LOGGER_INPUT="!!!! WARNING !!! WARNING - Line ${LINE_INDICATOR} in '${MULT_EXT_DETAILS_FILE}' is improper - hence  Skipped ";
	PGM_LOGGER_MAIN_MENU_OPTION=${OPTION_MAIN_MENU};
	PGM_LOGGER_SUB_MENU_OPTION=${OPTION_EXTRACTION_MENU};
	perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION};	
	
	echo -e ${COLR_BRT_GREEN} "${COS_FILLER_85_EQUALS}" ${COLR_END}
	echo $COS_FILLER_SPACES;
	echo " !!!! WARNING !!! WARNING  "
	echo " !!!! WARNING !!! WARNING - Line ${LINE_INDICATOR} in '${MULT_EXT_DETAILS_FILE}' is improper - hence Skipped "
	echo " !!!! WARNING !!! WARNING  "
	if [ "${SORC_CUST_SQL_CHOICE}" != "Y" ];then
		echo " !!!! WARNING !!! WARNING - Source Database           : ${SORC_DBS} "
		echo " !!!! WARNING !!! WARNING - Source Table              : ${SORC_TBL} "
	fi
	echo " !!!! WARNING !!! WARNING - Sampling/Extraction Limit : ${ROW_SAMPLING_LIMIT} "
	echo " !!!! WARNING !!! WARNING - Custom SQL Enabled        : ${SORC_CUST_SQL_CHOICE} "
	if [ "${SORC_CUST_SQL_CHOICE}" == "Y" ];then
		echo " !!!! WARNING !!! WARNING - Custom-SQL DB             : ${CUSTOM_SQL_DB} "
		echo " !!!! WARNING !!! WARNING - Custom-SQL Table          : ${CUSTOM_SQL_TBL} "
		echo " !!!! WARNING !!! WARNING - Custom-Query              : ${SORC_CUST_SQL} "
	fi	
	echo " !!!! WARNING !!! WARNING - Hive Settings             : ${HIVE_SETTINGS} "
	echo " !!!! WARNING !!! WARNING  "
	echo " !!!! WARNING !!! WARNING  "
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "${COS_FILLER_85_EQUALS}" ${COLR_END}
}
#----------------------------------------------------------------
# FUNCTION CALLING : Message to be displayed when mutliple extract 
#					 is PROPER due to the Extract Input File
#----------------------------------------------------------------
function perform_0008_extract_multiple_table_success
{
	PGM_LOGGER_INPUT=" !!!! SUCCESS !!! SUCCESS - Line ${LINE_INDICATOR} in '${MULT_EXT_DETAILS_FILE}' is proper - hence Processed ";
	PGM_LOGGER_MAIN_MENU_OPTION=${OPTION_MAIN_MENU};
	PGM_LOGGER_SUB_MENU_OPTION=${OPTION_EXTRACTION_MENU};
	perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION};	
	
	echo -e ${COLR_BRT_GREEN} "${COS_FILLER_85_EQUALS}" ${COLR_END}
	echo $COS_FILLER_SPACES;
	echo " !!!! SUCCESS !!! SUCCESS  "
	echo "${PGM_LOGGER_INPUT}"
	echo " !!!! SUCCESS !!! SUCCESS  "
	if [ "${SORC_CUST_SQL_CHOICE}" != "Y" ];then
			echo " !!!! SUCCESS !!! SUCCESS - Source Database           : ${SORC_DBS} "
			echo " !!!! SUCCESS !!! SUCCESS - Source Table              : ${SORC_TBL} "
	fi
	echo " !!!! SUCCESS !!! SUCCESS - Sampling/Extraction Limit : ${ROW_SAMPLING_LIMIT} "
	echo " !!!! SUCCESS !!! SUCCESS - Custom SQL Enabled        : ${SORC_CUST_SQL_CHOICE} "
	if [ "${SORC_CUST_SQL_CHOICE}" == "Y" ];then
		echo " !!!! SUCCESS !!! SUCCESS - Custom-SQL DB             : ${CUSTOM_SQL_DB} "
		echo " !!!! SUCCESS !!! SUCCESS - Custom-SQL Table          : ${CUSTOM_SQL_TBL} "
		echo " !!!! SUCCESS !!! SUCCESS - Custom-Query              : ${SORC_CUST_SQL} "
	fi	
	echo " !!!! SUCCESS !!! SUCCESS - Hive Settings             : ${HIVE_SETTINGS} "
	echo " !!!! SUCCESS !!! SUCCESS  "
	echo " !!!! SUCCESS !!! SUCCESS  "
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "${COS_FILLER_85_EQUALS}" ${COLR_END}
}
#----------------------------------------------------------------
# FUNCTION CALLING : Message to be displayed when mutliple load 
#					 is PROPER due to the Load Input/Parameter File
#----------------------------------------------------------------
function perform_0009_load_multiple_table_error
{
	PGM_LOGGER_INPUT=" !!!! WARNING !!! WARNING - Line ${LINE_INDICATOR} in '${MULT_LOD_DETAILS_FILE}' is improper - hence Skipping ";
	PGM_LOGGER_MAIN_MENU_OPTION=${OPTION_MAIN_MENU};
	PGM_LOGGER_SUB_MENU_OPTION=${OPTION_LOAD_MENU};
	perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION};	

	echo -e ${COLR_BRT_GREEN} "${COS_FILLER_85_EQUALS}" ${COLR_END}
	echo $COS_FILLER_SPACES;
	echo " !!!! WARNING !!! WARNING  "
	echo "${PGM_LOGGER_INPUT}"
	echo " !!!! WARNING !!! WARNING  "
	echo " !!!! WARNING !!! WARNING - Source FILENAME                   : ${FILENAME} "
	echo " !!!! WARNING !!! WARNING - Target Staging Database           : ${TARGT_STAGING_DB} "
	echo " !!!! WARNING !!! WARNING - Temporary Table                   : ${TEMP_TABLE} "
	echo " !!!! WARNING !!! WARNING - Target Table Partitioned ?        : ${PARTITION_FLAG} "
	echo " !!!! WARNING !!! WARNING - Target Table Partitioning Column  : ${TARGT_TBL_PART_COL} "
	echo " !!!! WARNING !!! WARNING - Pre-Staging Table                 : ${TARGT_PRE_TABLE} "
	echo " !!!! WARNING !!! WARNING - Target Table                      : ${TARGT_TABLE} "
	echo " !!!! WARNING !!! WARNING  "
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "${COS_FILLER_85_EQUALS}" ${COLR_END}
}
#----------------------------------------------------------------
# FUNCTION CALLING : Message to be displayed when mutliple load 
#					 is PROPER due to the Load Input/Parameter File
#----------------------------------------------------------------
function perform_0009_load_multiple_table_success
{
	PGM_LOGGER_INPUT=" !!!! SUCCESS !!! SUCCESS - Line ${LINE_INDICATOR} in '${MULT_LOD_DETAILS_FILE}' is Proper - hence Processed ";
	PGM_LOGGER_MAIN_MENU_OPTION=${OPTION_MAIN_MENU};
	PGM_LOGGER_SUB_MENU_OPTION=${OPTION_LOAD_MENU};
	perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION};	
	
	echo -e ${COLR_BRT_GREEN} "${COS_FILLER_85_EQUALS}" ${COLR_END}
	echo $COS_FILLER_SPACES;
	echo "${PGM_LOGGER_INPUT}"
	echo ""
	echo " !!!! SUCCESS !!! SUCCESS  "
	echo " !!!! SUCCESS !!! SUCCESS - Source FILENAME                   : ${FILENAME} "
	echo " !!!! SUCCESS !!! SUCCESS - Target Staging Database           : ${TARGT_STAGING_DB} "
	echo " !!!! SUCCESS !!! SUCCESS - Temporary Table                   : ${TEMP_TABLE} "
	echo " !!!! SUCCESS !!! SUCCESS - Target Table Partitioned ?        : ${PARTITION_FLAG} "
	echo " !!!! SUCCESS !!! SUCCESS - Target Table Partitioning Column  : ${TARGT_TBL_PART_COL} "
	echo " !!!! SUCCESS !!! SUCCESS - Pre-Staging Table                 : ${TARGT_PRE_TABLE} "
	echo " !!!! SUCCESS !!! SUCCESS - Target Table                      : ${TARGT_TABLE} "
	echo " !!!! SUCCESS !!! SUCCESS  "
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "${COS_FILLER_85_EQUALS}" ${COLR_END}
}
#----------------------------------------------------------------------------------------------------------------------------
# MAIN  FLOW OF PROGRAM
#----------------------------------------------------------------------------------------------------------------------------
mkdir -p ${LOG_DIR};

	perform_0000_globalvariable_declaration;

	rm -f $LOG_DIR/${PROGNAME}_${GLBVRB_APP}_log.txt;
	PGM_LOGGER_MAIN_MENU_OPTION="None";
	PGM_LOGGER_SUB_MENU_OPTION="None";
	perform_0000_logger ${PGM_LOGGER_INPUT} ${PGM_LOGGER_MAIN_MENU_OPTION} ${PGM_LOGGER_SUB_MENU_OPTION};
	
	perform_0000_user_permission_check;
	perform_0000_enable_kerberos;
	perform_invoke_main_menu;
