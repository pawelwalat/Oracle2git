#!/usr/bin/env python3

##########################################################################
### Oracle2GIT
### Author: Pawel Walat
### Current Version: 1.2
###
### Change Log:
###     1.0: 16-Jul-2019 - Initial version (Pawel Walat)
###     1.1: 23-Jul-2019 - Added logging, password masking, other improvements (Pawel Walat)
###     1.2: 22-Aug-2019 - Changed newlines to CRLF
###
### Before you use
###     pip install jpype1 (or use one from https://www.lfd.uci.edu/~gohlke/pythonlibs/)
###     pip install jaydebeapi
###
###    Please also ensure you have ojdbc6.jar
###    To generate executable file use: pyinstaller --onefile Oracle2GIT.py
##########################################################################

import sys
import os
import jpype
import jaydebeapi
import argparse
from datetime import datetime
import threading
import time
import logging

## Function to dump objects of given type to the directory
def dump_src(schema_name, object_type, directory, file_extension, conn,file_footer ='', chunks = 1, chunk_number=0):
    sql = """SELECT   object_name,
         DECODE (object_type,
                 'PACKAGE', 'PACKAGE_SPEC',
                 'PACKAGE BODY', 'PACKAGE_BODY',
                 'JAVA SOURCE', 'JAVA_SOURCE',
                 object_type)
            object_type,
         DBMS_METADATA.get_ddl (
            DECODE (object_type,
                    'PACKAGE', 'PACKAGE_SPEC',
                    'PACKAGE BODY', 'PACKAGE_BODY',
                    'JAVA SOURCE', 'JAVA_SOURCE',
                    'MATERIALIZED VIEW', 'MATERIALIZED_VIEW',
                    object_type),
            object_name,
            '{schema_name}'
         )
            text
  FROM   dba_objects
 WHERE       owner = '{schema_name}'
         AND object_type IN ('{object_type}')
         and mod(object_id,{chunks})={chunk_number}
         and (EDITIONABLE='Y' OR OBJECT_TYPE <> 'TYPE')
         AND object_id NOT IN (SELECT   purge_object FROM recyclebin)
         and (object_type <> 'TABLE' or exists (select 1 from dba_tab_cols where table_name=dba_objects.object_name and dba_tab_cols.owner=dba_objects.owner))
         AND NOT (object_type = 'TABLE'
                  AND EXISTS
                        (SELECT   1
                           FROM   dba_Mviews
                          WHERE   owner = dba_objects.owner
                                  AND mview_name = dba_objects.object_name))"""
    if object_type=='JOB':
        sql="""SELECT substr(what,instr(what,'.')+1,length(what)-(instr(what,'.')+1)) object_name, 'JOB' object_type, 'DECLARE 
            X NUMBER; 
            BEGIN
                SYS.DBMS_JOB.SUBMIT 
                ( job        =>  X
                  ,what      =>  '''||WHAT||'''
                  ,next_date => to_date('''||to_char(next_date,'dd/mm/yyyy hh24:mi:ss')||''',''dd/mm/yyyy hh24:mi:ss'') 
                  ,interval  => '''||interval||''' 
                  ,no_parse  => FALSE 
                  );
                  SYS.DBMS_OUTPUT.PUT_LINE(''JobNumber is: ''||to_char(x)); 
            commit; 
            end;
            '  text FROM all_jobs where priv_user='{schema_name}'"""

    if object_type=='MATERIALIZED VIEW LOG':
        sql  = """SELECT master, 'MATERIALIZED VIEW LOG' object_type,
        DBMS_METADATA.GET_DDL('MATERIALIZED_VIEW_LOG', log_table, schema=> '{schema_name}') DDL
        FROM
        dba_MVIEW_LOGS
        where
        LOG_OWNER = '{schema_name}'"""
    if object_type == 'REF CONSTRAINT':
        sql ="""select 
        constraint_name, 'CONSTRAINT', DBMS_METADATA.get_ddl ('REF_CONSTRAINT', constraint_name, owner)
         from dba_constraints
        where owner='{schema_name}'
        and mod(ORA_HASH(CONCAT(constraint_name, table_name)),{chunks})={chunk_number}        
        and constraint_type='R'"""

    sql = sql.replace('{schema_name}',schema_name)
    sql = sql.replace('{object_type}', object_type)
    sql = sql.replace('{chunk_number}', str(chunk_number))
    sql = sql.replace('{chunks}', str(chunks))
    if chunks != 1:
        logger.info("Processing object type: " + str(object_type)+" Chunk "+str(chunk_number)+"/"+str(chunks))
    else:
        logger.info("Processing object type: "+object_type)
    try:
        jpype.attachThreadToJVM()
        cur = conn.cursor()
        if  object_type == 'TRIGGER':
            cur.execute("begin dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'SQLTERMINATOR', true); end;")
        else:
            cur.execute("begin dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'SQLTERMINATOR', false); end;")
        cur.execute(sql)
        result = cur.fetchall()
        for row in result:
            if not os.path.exists(directory):
                os.makedirs(directory)
            file = open(os.path.join(directory,row[0]+file_extension), 'wb')
            if object_type == 'JOB':
                file.write((row[2]+ file_footer).replace(r'\n', '\r\n'))
            else:
                file.write((row[2].getSubString(1,row[2].length())+file_footer).encode('utf-8').replace( b'\r\n',b'\n').replace(b'\n', b'\r\n'))
            file.close()
        cur.close()
    except Exception as e:
        logger.error(str(chunk_number) + 'ex')
        logger.error("Error: " + str(e))
        sys.exit(1)

def dump_src_threads(threads, schema_name, object_type, directory, file_extension, conn,file_footer =''):
    x = []
    for i in range(0, threads, 1):
        x.append(threading.Thread(target=dump_src, args=(
            schema_name, object_type, directory,file_extension, conn[i], file_footer, threads,
        i)))
    for i in x:
        i.start()
        time.sleep(1)
    logger.info("Waiting for threads to finish work...")
    for i in x:
        i.join()

## Program code
use_sid=1
parser = argparse.ArgumentParser(description='Oracle2GIT')
parser.add_argument('output_directory', metavar='output_directory', type=str, help='Destination directory for dump')
parser.add_argument('hostname', metavar='hostname<:port>', type=str, help='Oracle DB hostname <and port>')
parser.add_argument('SID', metavar='SID', type=str, help='Oracle DB Service Name or SID (use --use-sid)')
parser.add_argument('username', metavar='username', type=str, help='Oracle DB username')
parser.add_argument('schema_name', metavar='schema_name', type=str, help='Schema name to be dumped')
parser.add_argument('--use-sid', dest='use_sid', action='store_const', const=use_sid, help='connect using SID instead of Service Name')
parser.add_argument('--jdbc-dir', dest='jdbc_dir', action='store', help='specify where jdbc library is located')
parser.add_argument('--password', '-p', dest='password', action='store', help='Oracle user password (optional)')

args = parser.parse_args()
path, filename = os.path.split(os.path.realpath(__file__))

if args.password is None:
    password = input("What is the password for "+args.hostname+'/'+args.SID+', user: '+args.username+'? ')
else:
    password=args.password

#Check if directory exists
try:
    if os.path.exists(args.output_directory):
        os.rename(args.output_directory, os.path.normpath(args.output_directory)+"_bkp_"+datetime.now().strftime("%Y%m%d%H%M%S"))
    os.makedirs(args.output_directory)
except Exception as e:
    print("Error: " + str(e))
    sys.exit(1)

#Set up logger
logger = logging.getLogger('Oracle2GIT')
hdlr1 = logging.FileHandler(os.path.normpath(args.output_directory)+'/Oracle2GIT.log')
hdlr2 = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr1.setFormatter(formatter)
hdlr2.setFormatter(formatter)
logger.addHandler(hdlr1)
logger.addHandler(hdlr2)
logger.setLevel(logging.INFO)
logger.info('Oracle2GIT started.')
logger.info('Working Directory set to: '+os.path.normpath(args.output_directory))

# Checking for JDBC library
jdbcfilename = None
for version in ['6']:
    testjdbcfilename = "ojdbc"+version+".jar"
    if args.jdbc_dir != None and os.path.isfile(os.path.join(args.jdbc_dir,testjdbcfilename)):
        jdbcfilename = os.path.join(os.path.dirname(os.path.realpath(__file__)), testjdbcfilename)
        break
    elif os.path.isfile(os.path.join(os.getcwd(),testjdbcfilename)):
        jdbcfilename = os.path.join(os.getcwd(),testjdbcfilename)
        break
    elif os.path.isfile(os.path.join(os.path.dirname(os.path.realpath(__file__)),testjdbcfilename)):
        jdbcfilename = os.path.join(os.path.dirname(os.path.realpath(__file__)), testjdbcfilename)
        break
    elif os.path.isfile(os.path.join(path,testjdbcfilename)):
        jdbcfilename = os.path.join(path, testjdbcfilename)
        break
if jdbcfilename == None:
    logger.error("Cannot find JDBC library. Provide location with --jdbc-dir parameter.")
    sys.exit("Cannot find JDBC library. Provide location with --jdbc-dir parameter.")

logger.info("Using JDBC library: "+jdbcfilename)
jHome = jpype.getDefaultJVMPath()
jpype.startJVM(jHome, '-Djava.class.path='+jdbcfilename)

logger.info("Connecting to the DB...")
try:
    connection_string =  'jdbc:oracle:thin:'+args.username+'/'+password+'@'+args.hostname+'/'+args.SID
    logger.info("Connection string: " + 'jdbc:oracle:thin:'+args.username+'/'+'**********'+'@'+args.hostname+'/'+args.SID)
    conn = []
    for i in range(0,16):
        conn.append(jaydebeapi.connect('oracle.jdbc.driver.OracleDriver', connection_string))
except Exception as e:
	logger.error("Error: "+str(e))
	sys.exit(1)

dump_src_threads(16, args.schema_name,'INDEX', os.path.join(os.path.normpath(args.output_directory),'Indexes'),'.idx', conn,';'+os.linesep+'/')
dump_src_threads(16, args.schema_name,'TABLE', os.path.join(os.path.normpath(args.output_directory),'Tables'),'.tab', conn,';'+os.linesep+'/')
dump_src_threads(16, args.schema_name,'PACKAGE', os.path.join(os.path.normpath(args.output_directory),'Packages'),'.pks' ,conn,'/')
dump_src_threads(16, args.schema_name,'PACKAGE BODY', os.path.join(os.path.normpath(args.output_directory),'Packages'),'.pkb', conn,'/')
dump_src_threads(8, args.schema_name,'PROCEDURE', os.path.join(os.path.normpath(args.output_directory),'Procedures'),'.prc', conn, '/')
dump_src_threads(8, args.schema_name,'FUNCTION', os.path.join(os.path.normpath(args.output_directory),'Functions'),'.fnc', conn,'/')
dump_src_threads(8,args.schema_name,'SEQUENCE', os.path.join(os.path.normpath(args.output_directory),'Sequences'),'.seq', conn,';'+os.linesep+'/')
dump_src_threads(8,args.schema_name,'TRIGGER', os.path.join(os.path.normpath(args.output_directory),'Triggers'),'.trg', conn,''+os.linesep+'/')
dump_src_threads(8,args.schema_name,'VIEW', os.path.join(os.path.normpath(args.output_directory),'Views'),'.vw', conn,';'+os.linesep+'/')
dump_src_threads(8,args.schema_name,'REF CONSTRAINT', os.path.join(os.path.normpath(args.output_directory),'Ref_constraint'),'.sql', conn,';'+os.linesep+'/')
dump_src(args.schema_name,'SYNONYM', os.path.join(os.path.normpath(args.output_directory),'Synonyms'),'.syn', conn[0],';'+os.linesep+'/')
dump_src(args.schema_name,'JOB', os.path.join(os.path.normpath(args.output_directory),'Jobs'),'.job', conn[0])
dump_src(args.schema_name,'JAVA SOURCE', os.path.join(os.path.normpath(args.output_directory),'Java_source'),'.java', conn[0])
dump_src(args.schema_name,'MATERIALIZED VIEW LOG', os.path.join(os.path.normpath(args.output_directory),'Materialized_view_logs'),'.sql', conn[0],';'+os.linesep+'/')
dump_src(args.schema_name,'TYPE', os.path.join(os.path.normpath(args.output_directory),'Types'),'.typ', conn[0],'/')
dump_src(args.schema_name,'MATERIALIZED VIEW', os.path.join(os.path.normpath(args.output_directory),'Materialized_views'),'.mv', conn[0],';'+os.linesep+'/')

logger.info('Oracle2GIT finished work.')

