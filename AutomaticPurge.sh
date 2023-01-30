#!/bin/sh
#--------------------------------------------------------------------------------------------------#
#THIS SCRIPT DROPS THE DATABASE AND THE TABLE CONTAINED IN THE HIVE	                               #
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
CURR_DIR=`pwd`;
CURR_TMSTP=`echo $(date +%Y-%m-%d" "%H:%M:%S)| sed -e 's/-//g' -e 's/://g' -e 's/ //g' | cut -c1-14`
TABLE_LIST_FILE_TEMP=${CURR_DIR}/db_drop_${CURR_TMSTP}.txt;
LOG_FILE=${CURR_DIR}/LOG_db_drop_${CURR_TMSTP}.txt;
TABLE_LIST_FILE=${CURR_DIR}/db_drop_${CURR_TMSTP}.hql
export COLR_BRT_YELLOW='\e[1;33m'
export COLR_BRT_GREEN='\033[1;32m';
export COLR_END='\e[0m' ;
CMND_TRNC=" TRUNCATE TABLE ";
CMND_DROP=" DROP TABLE IF EXISTS "
COS_FILLER_SPACES=" ";
COS_FILLER_85_EQUALS="====================================================================================";
COS_FILLER_85_MINUS="------------------------------------------------------------------------------------";
COS_FILLER_01_SEMICOLON=";"

#----------------------------------------------------------------
# FUNCTION CALLING : Exits the program on User's Request
#----------------------------------------------------------------
function perform_0000_log_messages_to_file
{
	echo -e ${COLR_BRT_GREEN} "${LOG_MESSAGE}"${COLR_END};
	echo $COS_FILLER_SPACES >> ${LOG_FILE}
	echo "${LOG_MESSAGE}" >> ${LOG_FILE}
}
#----------------------------------------------------------------
# FUNCTION CALLING : Checks for the current executor and decides
#                    to procced or terminate the execution
#----------------------------------------------------------------
function perform_0000_user_permission_check
{
if ([ "$CURR_SERVER" == "hpchdp2e" ] && [ "$CURR_USER" != "iapxheam" ]);then
	echo $COS_FILLER_85_EQUALS;
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN}" ERROR * ERROR * ERROR * ERROR *-------------------------------------------------------------  ERROR * ERROR * ERROR * ERROR *  "${COLR_END};
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} " User '$CURR_USER' IS NOT AUTHORISED TO EXECUTE THE SCRIPT '$PROGNAME' IN THE SERVER '$CURR_SERVER' "${COLR_END};
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} " ERROR * ERROR * ERROR * ERROR *-------------------------------------------------------------  ERROR * ERROR * ERROR * ERROR *  "${COLR_END};
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_85_EQUALS;
	exit 1;
fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Enabling Kerberos
#----------------------------------------------------------------
function perform_0000_enable_kerberos
{
clear;
kinit -k -t /s/$CURR_USER/$CURR_USER.keytab $CURR_USER; RS_KINIT=`echo $?`;
if  [ $RS_KINIT -eq 0 ];then
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_SPACES;
	LOG_MESSAGE=" perform_0000_enable_kerberos --> SUCCESS *SUCCESS *  Enabling Kerberos Successful SUCCESS *SUCCESS "
	perform_0000_log_messages_to_file;
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_SPACES;
else
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} " ERROR * ERROR * ERROR *  Enabling Kerberos Failed  ERROR * ERROR * ERROR *  "${COLR_END};
	LOG_MESSAGE=" perform_0000_enable_kerberos --> Error While executing : kinit -k -t /s/$CURR_USER/$CURR_USER.keytab $CURR_USER "
	perform_0000_log_messages_to_file;
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_SPACES;
	exit 1;
fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Exits the program on User's Request
#----------------------------------------------------------------
function perform_0000_exit_program
{
	clear;
	rm -f ${TABLE_LIST_FILE};
	rm -f ${TABLE_LIST_FILE_TEMP};
	echo -e ${COLR_BRT_GREEN}$COS_FILLER_85_EQUALS ${COLR_END};
	echo $COS_FILLER_SPACES;
	LOG_MESSAGE="  * * THANK YOU FOR USING THE SCA-C DATABASE PURGE TOOL , SEE YA LATER !!!! * * * "
	perform_0000_log_messages_to_file;
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN}$COS_FILLER_85_EQUALS ${COLR_END};
	exit 0;
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
	echo -e ${COLR_BRT_GREEN}"  *  *  *       WELCOME   TO   SCA-C  DATA   DATABASE PURGE   MAIN-MENU         *  *  * " ${COLR_END};
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN}"                      Enter your Option ( 1 till 3 Only )"${COLR_END}
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_85_EQUALS;
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "	                   1.      SINGLE DATABASE PURGE <interactive mode> "${COLR_END}
	echo $COS_FILLER_SPACES;
	echo -e ${COLR_BRT_GREEN} "	                   2.      MULTIPLE DATABASE PURGE <Batch-mode>"${COLR_END}
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
	perform_0000_single_db_purge;
	perform_invoke_main_menu;
	fi
	
	if [ "$OPTION_MAIN_MENU" == "2" ];then
	echo "Entered Main Menu Option is $OPTION_MAIN_MENU!"
	perform_0000_multiple_db_purge;
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
	perform_invoke_main_menu;
	fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Get the Database to be Purged from User
#----------------------------------------------------------------
function perform_0001_get_database_name_to_purge_from_user
{
clear;
	echo -en ${COLR_BRT_YELLOW}"Enter the Database to be Dropped : " ${COLR_END}
	read DB_NAME
	if [ "x$DB_NAME" == "x" ];then
	   echo "Please enter valid Database Name :!"
	   exit 1
	fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Get the Database(s) to be Purged from File
#----------------------------------------------------------------
function perform_0002_get_database_name_to_purge_from_file
{
clear;
echo $COS_FILLER_SPACES;
		echo -en ${COLR_BRT_YELLOW} "Enter the Absolute Input File Containing the Mutiple DB details (eg: /s/iapxhtam/input.txt) : " ${COLR_END}
		read MULT_DB_DETAILS_FILE
		if [ "x$MULT_DB_DETAILS_FILE" == "x" ];then
		   echo "Please enter valid Absolute Input File Contiaining the Database details (eg: /s/iapxhtam/input.txt) !"
		   exit 1
		else
			sed -i '/^$/d' ${MULT_DB_DETAILS_FILE};
			if [ -s "$MULT_DB_DETAILS_FILE" ];then
				echo "";
			else
				echo " !!!! ERROR !!! ERROR  "
				echo " !!!! ERROR !!! ERROR - File '${MULT_DB_DETAILS_FILE}' does not exist [or] is empty '"
				echo " !!!! ERROR !!! ERROR  "
				exit 1;
			fi
		fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Get the list of tables under the Database
#----------------------------------------------------------------
function perform_0003_get_table_list_under_db	
{
LOG_MESSAGE=" perform_0003_get_table_list_under_db --> show tables in ${DB_NAME}${COS_FILLER_01_SEMICOLON}"
perform_0000_log_messages_to_file;

beeline -e "show tables in ${DB_NAME}${COS_FILLER_01_SEMICOLON}";RS_DISP_TBL_IN_DB=`echo $?`;

rm -f ${TABLE_LIST_FILE_TEMP}

if [ "${RS_DISP_TBL_IN_DB}" == "0" ];then 

	echo -e ${COLR_BRT_GREEN}"show tables in ${DB_NAME}${COS_FILLER_01_SEMICOLON}"${COLR_END};
	hive -e  "show tables in ${DB_NAME}${COS_FILLER_01_SEMICOLON}" >> ${TABLE_LIST_FILE_TEMP}
	
	if [ -s "${TABLE_LIST_FILE_TEMP}" ];then
	IFS=''
		while read line
		do		
		echo "${CMND_DROP} ${DB_NAME}.$line${COS_FILLER_01_SEMICOLON}" >> ${TABLE_LIST_FILE};	
		LOG_MESSAGE=" perform_0003_get_table_list_under_db --> ${CMND_DROP} ${DB_NAME}.$line${COS_FILLER_01_SEMICOLON}"
		perform_0000_log_messages_to_file;
		
		done < ${TABLE_LIST_FILE_TEMP}
	else	
		echo $COS_FILLER_SPACES;
		LOG_MESSAGE=" perform_0003_get_table_list_under_db -->  WARNING * WARNING * WARNING *  Empty Database ! ! !  "
		perform_0000_log_messages_to_file;
		echo $COS_FILLER_SPACES;
	fi	
else
	echo $COS_FILLER_85_EQUALS;
	echo $COS_FILLER_SPACES;
	LOG_MESSAGE=" perform_0003_get_table_list_under_db --> ERROR * ERROR * ERROR *  Database ${DB_NAME} does NOT Exist "
	perform_0000_log_messages_to_file;
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_85_EQUALS
	#exit 1;	
fi		
}
#----------------------------------------------------------------
# FUNCTION CALLING : Drop the Tables
#----------------------------------------------------------------
function perform_0004_drop_tables	
{
	echo -e ${COLR_BRT_GREEN}"${TABLE_LIST_FILE}"${COLR_END}; 
	
	cp ${TABLE_LIST_FILE} ${TABLE_LIST_FILE}_TEMP;
	
	sed -i '/^$/d' ${TABLE_LIST_FILE_TEMP};
	if [ -s "$TABLE_LIST_FILE" ];then
		beeline -f  "${TABLE_LIST_FILE}_TEMP"
		rm ${TABLE_LIST_FILE}_TEMP
	fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Drop the Tables
#----------------------------------------------------------------
function perform_0005_drop_database	
{
LOG_MESSAGE=" perform_0005_drop_database --> drop database ${DB_NAME}${COS_FILLER_01_SEMICOLON} "
perform_0000_log_messages_to_file;

beeline -e  "drop database ${DB_NAME}${COS_FILLER_01_SEMICOLON}";RS_DROP_DB=`echo $?`;
	
if [ "${RS_DROP_DB}" == "0" ];then
	
	echo $COS_FILLER_85_EQUALS;
	echo $COS_FILLER_SPACES;
	LOG_MESSAGE=" perform_0005_drop_database --> Database ${DB_NAME} - Dropped successfully "
	perform_0000_log_messages_to_file;

	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_85_EQUALS
	rm -f ${TABLE_LIST_FILE_TEMP}
else 

	echo $COS_FILLER_85_EQUALS;
	echo $COS_FILLER_SPACES;
	LOG_MESSAGE=" perform_0005_drop_database --> There was an error in dropping the Database ${DB_NAME}- make sure the DB Exists and Empty "
	perform_0000_log_messages_to_file;
	echo $COS_FILLER_SPACES;
	echo $COS_FILLER_85_EQUALS;
fi
}
#----------------------------------------------------------------
# FUNCTION CALLING : Addressing Single Database Purge Needs
#----------------------------------------------------------------
function perform_0000_single_db_purge	
{
	perform_0001_get_database_name_to_purge_from_user
	perform_0003_get_table_list_under_db
	perform_0004_drop_tables
	perform_0005_drop_database
}
#----------------------------------------------------------------
# FUNCTION CALLING : Addressing Multiple Database Purge Needs
#----------------------------------------------------------------
function perform_0000_multiple_db_purge	
{
	perform_0002_get_database_name_to_purge_from_file
	IFS=''
		while read line
		do
			DB_NAME=`echo $line`;
			perform_0003_get_table_list_under_db
			echo "${COS_FILLER_SPACES}" >> ${TABLE_LIST_FILE};
		done < ${MULT_DB_DETAILS_FILE}
		
		perform_0004_drop_tables
		
		while read line
		do
			DB_NAME=`echo $line`;
			perform_0005_drop_database
			echo "${COS_FILLER_SPACES}" >> ${TABLE_LIST_FILE};
		done < ${MULT_DB_DETAILS_FILE}
}
#----------------------------------------------------------------------------------------------------------------------------
# MAIN  FLOW OF PROGRAM
#----------------------------------------------------------------------------------------------------------------------------
LOG_MESSAGE=" Process Timestamp : ${CURR_TMSTP}"
perform_0000_log_messages_to_file;

perform_0000_user_permission_check;
perform_0000_enable_kerberos;
perform_invoke_main_menu;
