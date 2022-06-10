#!/usr/bin/env python3

#pip install xlsxwriter
#pip install pandas

# tool imports
import os.path
from os import path
import sys
import re
import math

# Astra Spark Migration Data Model Extrator
version = "1.0.0"

# Retrieve default values
cfg_base_array = {
  'beta' :                          'false',
  'source_host':                    'localhost',
  'source_read_consistancy_level' : 'LOCAL_QUORUM',
  'astra_read_consistancy_level' :  'LOCAL_QUORUM',
  'max_retries' :                   '10',
  'read_rate_limit' :               '40000',
  'write_rate_limit' :              '40000',
  'split_size' :                    '10000',
  'batch_size' :                    '5',
  'print_stats_after' :             '100000',
  'counter_table' :                 'false'
}

field_type_array = {
  'ascii':    '0',
  'text':     '0',
  'varchar':  '0',
  'int':      '1',
  'varint':   '1', # confirm
  'smallint': '1',
  'bigint':   '2',
  'counter':  '2',
  'double':   '3',
  'time':     '4',
  'timestamp':'4',
  'map':      '5',
  'list':     '6',
  'blob':     '7',
  'set':      '8',
  'uuid':     '9',
  'timeuuid': '9',
  'boolean':  '10',
  'tuple':    '11',
  'float':    '12',
  'tinyint':  '13',
  'decimal':  '14',
  'date':     '1',
  'inet':     '0'
  
}
system_keyspace = ['OpsCenter','dse_insights_local','solr_admin','test','dse_system','dse_analytics','system_auth','system_traces','system','dse_system_local','system_distributed','system_schema','dse_perf','dse_insights','dse_security','dse_system','killrvideo','dse_leases','dsefs_c4z','HiveMetaStore','dse_analytics','dsefs','spark_system']
tp_tbl_data = {
    'Materialized Views':{},
    'Secondary Indexes':{},
    'Storage-Attached Indexes':{}
}

def field_type_comment(tbl,fieldName,fieldType):
  if fieldType=='date':
    print('Alert for '+tbl+'.'+fieldName+'\n\tValues of the date type are encoded as 32-bit unsigned integers representing a number of days with “the epoch” at the center of the range (2^31). Epoch is January 1st, 1970\n\tFor timestamps, a date can be input either as an integer or using a date string. In the later case, the format should be yyyy-mm-dd (so 2011-02-03 for instance).\n\tAdditional customization required for this table.\n')
  elif fieldType=='decimal':
    print('Alert for '+tbl+'.'+fieldName+'\n\tValues of the decimal type require specific variable-precision at the field level./n/tAdditional customization required for this table.\n')
  elif fieldType=='double':
    print('Alert for '+tbl+'.'+fieldName+'\n\tValues of the double type require specific variable-precision at the field level./n/tAdditional customization required for this table.\n')
  elif fieldType=='float':
    print('Alert for '+tbl+'.'+fieldName+'\n\tValues of the float type require additonal work at the field level./n/tAdditional customization required for this table.\n')
  elif fieldType=='time' or fieldType=='timestamp':
    print('Alert for '+tbl+'.'+fieldName+'\n\tValues of the time type are encoded as 64-bit signed integers representing the number of nanoseconds since midnight.\n\tFor timestamps, a time can be input either as an integer or using a string representing the time. In the later case, the format should be hh:mm:ss[.fffffffff] (where the sub-second precision is optional and if provided, can be less than the nanosecond). So for instance, the following are valid inputs for a time:\n\t\t08:12:54\n\t\t08:12:54.123\n\t\t08:12:54.123456\n\t\t08:12:54.123456789\n\tAdditional customization required for this table.\n')


# 0: String [ascii, text, varchar]
# 1: Integer [int, smallint]
# 2: Long [bigint]
# 3: Double [double]
# 4: Instant [time, timestamp]
# 5: Map (separate type by %) [map] - Example: 5%1%0 for map<int, text>
# 6: List (separate type by %) [list] - Example: 6%0 for list<text>
# 7: ByteBuffer [blob]
# 8: Set (seperate type by %) [set] - Example: 8%0 for set<text>
# 9: UUID [uuid, timeuuid]
#10: Boolean [boolean]
#11: TupleValue [tuple]
#12: Float (float)
#13: TinyInt [tinyint]
#14: BigDecimal (decimal)

# initialize script variables
field_types = field_type_array.keys()
migrate_ks = ''
show_help = ''
target_field_type = []
migrate_tbl_data = {}
target_table = []
dm_path = ''
cf_file = 'sparkConfigDefaults.txt'
template_file = 'sparkConfTemplate.txt'
fieldData = {}
cfg_array = {}
schema_name = 'schema'

def add_tp_tbl(gr,ks,tbl,src_ks,src_tbl):
  if src_ks not in system_keyspace:
    try:
      type(tp_tbl_data[gr][src_ks])
    except:
      tp_tbl_data[gr][src_ks]={}
    try:
      type(tp_tbl_data[gr][src_ks][src_tbl])
    except:
      tp_tbl_data[gr][src_ks][src_tbl] = []
    if (ks+'.'+tbl) not in tp_tbl_data[gr][src_ks][src_tbl]:
      tp_tbl_data[gr][src_ks][src_tbl].append(ks+'.'+tbl)

def process_field(tbl,fieldName,fieldType,cql=''):
  if 'map<' in fieldType:
    mapData = fieldType.split('<')[1].split('>')[0].split(',')
    fieldValue = field_type_array['map']
    for mapType in mapData:
      fieldValue += '%' + field_type_array[mapType.strip()]
      field_type_comment(tbl,fieldName,mapType.strip())
  elif 'set<' in fieldType:
    mapData = fieldType.split('<')[1].split('>')[0].split(',')
    fieldValue = field_type_array['set']
    for mapType in mapData:
      fieldValue += '%' + field_type_array[mapType.strip()]
      field_type_comment(tbl,fieldName,mapType.strip())
  elif 'list<' in fieldType:
    mapData = fieldType.split('<')[1].split('>')[0].split(',')
    fieldValue = field_type_array['list']
    for mapType in mapData:
      fieldValue += '%' + field_type_array[mapType.strip()]
      field_type_comment(tbl,fieldName,mapType.strip())
  elif fieldType in field_types:
    fieldValue = field_type_array[fieldType]
  else:
    exit('Error: unknown field type: ' + fieldType +'\n'+cql)
  cfg_array['fields'] += fieldName
  cfg_array['field_types'] += fieldValue
  
  if fieldType == 'counter':
    cfg_array['counter_table'] = 'true'
    # more work here
  elif fieldType == 'map' or fieldType == 'list' or fieldType == 'set':
    exit('Error:  Build fieldType: ' + fieldType)
  field_type_comment(tbl,fieldName,fieldType)

# communicate command line help
for argnum,arg in enumerate(sys.argv):
  if(arg=='-h' or arg =='--help'):
    help_content = \
      '\n'\
      'Script for creating migration support files\n'\
      'usage: autoSparkConfig.py -k KEYSPACE [-p PATH_MIGRATION_SUPPORT_FILES] [-c] [-t TABLE1] [-t TABLE2]\n'\
      'optional arguments:\n'\
      '-v, --version          Version\n'\
      '-h, --help             This help info\n'\
      '-p, --path             Path to data model file\n'\
      '-s, --schema           Name of schema file - Default schema\n'\
      '-k, --keyspace         *Required: keyspace\n'\
      '-c, --counter          Generates files for tables with counters\n'\
      '-t, --table            Generates file for table(s)\n'\
      '\n'\
      'Configuration Elements\n'\
      '-cf, --config          Configuration File - Default: migrateConfig.txt\n'\
      '     *Required elements in Config File\n'\
      '       source_host\n'\
      '       source_username\n'\
      '       source_password\n'\
      '       astra_scb\n'\
      '       astra_username\n'\
      '       astra_password\n'\
      '     Optional Elements in Config File\n'\
      '       beta                            Default: '+cfg_base_array['beta']+'\n'\
      '       source_read_consistancy_level   Default: '+cfg_base_array['source_read_consistancy_level']+'\n'\
      '       astra_read_consistancy_level    Default: '+cfg_base_array['astra_read_consistancy_level']+'\n'\
      '       max_retries                     Default: '+cfg_base_array['max_retries']+'\n'\
      '       read_rate_limit                 Default: '+cfg_base_array['read_rate_limit']+'\n'\
      '       write_rate_Limit                Default: '+cfg_base_array['write_rate_limit']+'\n'\
      '       split_size                      Default: '+cfg_base_array['split_size']+'\n'\
      '       batch_size                      Default: '+cfg_base_array['batch_size']+'\n'\
      '       print_stats_after               Default: '+cfg_base_array['print_stats_after']+'\n'\
      '\n'

    exit(help_content)
  elif(arg=='-v' or arg =='--version'):
    exit("Version " + version)
  elif(arg=='-s' or arg =='--schema'):
    schema_name = sys.argv[argnum+1]
  elif(arg=='-k' or arg =='--keyspace'):
    migrate_ks = sys.argv[argnum+1]
  elif(arg=='-c' or arg =='--counter'):
    target_field_type.append('counter')
  elif(arg=='-p' or arg =='--path'):
    dm_path = sys.argv[argnum+1]
  elif(arg=='-t' or arg =='--table'):
    target_table.append(sys.argv[argnum+1])
  elif(arg=='-cf' or arg =='--config'):
    cf_file = sys.argv[argnum+1]

if (migrate_ks==''):
  exit("keyspace required")

info_box = 'Astra Spark Migration Data Model Extrator\n'\
              'Version '+version+'\n'\
              'Supported data in separate spreadsheet tabs'\
 
# initialize database vaariables
is_index = 0
ks_array = []
count = 0
row={}
end_row={}

# collect and analyze schema
ks = ''
tbl = ''
tbl_data = {}

if path.isfile(dm_path + schema_name):
  schemaFile = open(dm_path + schema_name, 'r')
for line in schemaFile:
  line = line.strip('\n').strip()
  if (line==''): tbl=''
  elif("CREATE KEYSPACE" in line):
    prev_ks = ks
    ks = line.split()[2].strip('"')
    tbl_data[ks] = {'cql':line,'table':{}}
    tbl=''
  elif ks != '' and ks==migrate_ks:
    if('CREATE INDEX' in line):
      prev_tbl = tbl
      tbl = line.split()[2].strip('"')
      tbl_data[ks]['table'][tbl] = {'type':'Index', 'cql':line}
      src_ks = line.split('ON')[1].split('.')[0].strip().strip('"')
      src_tbl = line.split('ON')[1].split('.')[1].split()[0].strip()
      add_tp_tbl('Secondary Indexes',ks,tbl,src_ks,src_tbl)
      tbl=''
    elif('CREATE CUSTOM INDEX' in line):
      prev_tbl = tbl
      tbl = line.split()[3].strip('"')
      tbl_data[ks]['table'][tbl] = {'type':'Storage-Attached Index', 'cql':line}
      src_ks = line.split('ON')[1].split('.')[0].strip().strip('"')
      src_tbl = line.split('ON')[1].split('.')[1].split()[0].strip()
      add_tp_tbl('Storage-Attached Indexes',ks,tbl,src_ks,src_tbl)
      tbl=''
    elif('CREATE TYPE' in line):
      prev_tbl = tbl
      tbl_line = line.split()[2].strip('"')
      tbl = tbl_line.split('.')[1].strip().strip('"')
      tbl_data[ks]['table'][tbl] = {'type':'Type', 'cql':line}
      tbl_data[ks]['table'][tbl]['field'] = {}
    elif('CREATE AGGREGATE' in line):
      prev_tbl = tbl
      if 'IF NOT EXISTS' in line:
        tbl = line.split()[5].strip('"')
      else:
          tbl = line.split()[2].strip('"')
      tbl_data[ks]['table'][tbl] = {'type':'UDA', 'cql':line}
      tbl_data[ks]['table'][tbl]['field'] = {}
      try:
        warnings['Astra Guardrails']['User-Defined Aggregate'].append = 'UDA '+tbl+' in '+ks
      except:
        warnings['Astra Guardrails']['User-Defined Aggregate'] = ['UDA '+tbl+' in '+ks]
    elif('CREATE OR REPLACE FUNCTION' in line):
      prev_tbl = tbl
      tbl = line.split()[4].strip('"')
      tbl_data[ks]['table'][tbl] = {'type':'UDF', 'cql':line}
      tbl_data[ks]['table'][tbl]['field'] = {}
      try:
        warnings['Astra Guardrails']['User-Defined Function'].append = 'UDF '+tbl+' in '+ks
      except:
        warnings['Astra Guardrails']['User-Defined Function'] = ['UDF '+tbl+' in '+ks]
    elif 'CREATE FUNCTION' in line:
      prev_tbl = tbl
      tbl = line.split()[2].strip('"')
      tbl_data[ks]['table'][tbl] = {'type':'UDF', 'cql':line}
      tbl_data[ks]['table'][tbl]['field'] = {}
      try:
        warnings['Astra Guardrails']['User-Defined Function'].append = 'UDF '+tbl+' in '+ks
      except:
        warnings['Astra Guardrails']['User-Defined Function'] = ['UDF '+tbl+' in '+ks]
    elif('CREATE TABLE' in line):
      prev_tbl = tbl
      tbl_line = line.split()[2].strip('"')
      tbl = tbl_line.split('.')[1].strip().strip('"')
      tbl_data[ks]['table'][tbl] = {'type':'Table', 'cql':line}
      tbl_data[ks]['table'][tbl]['field'] = {}
    elif('CREATE MATERIALIZED VIEW' in line ):
      prev_tbl = tbl
      tbl_line = line.split()[3].strip('"')
      tbl = tbl_line.split('.')[1].strip().strip('"')
      tbl_data[ks]['table'][tbl] = {'type':'Materialized Views', 'cql':line}
      tbl_data[ks]['table'][tbl]['field'] = {}
    if (tbl !=''):
      if('FROM' in line and tbl_data[ks][tbl]['type']=='Materialized Views'):
        src_ks = line.split('.')[0].split()[1].strip('"')
        src_tbl = line.split('.')[1].strip('"')
        add_tp_tbl('Materialized Views',ks,tbl,src_ks,src_tbl)
      elif('PRIMARY KEY' in line):
        if(line.count('(') == 1):
          tbl_data[ks]['table'][tbl]['pk'] = [line.split('(')[1].split(')')[0].split(', ')[0]]
          tbl_data[ks]['table'][tbl]['cc'] = line.split('(')[1].split(')')[0].split(', ')
          del tbl_data[ks]['table'][tbl]['cc'][0]
        elif(line.count('(') == 2):
          tbl_data[ks]['table'][tbl]['pk'] = line.split('(')[2].split(')')[0].split(', ')
          tbl_data[ks]['table'][tbl]['cc'] = line.split('(')[2].split(')')[1].lstrip(', ').split(', ')
        elif(line.split()[2]=='PRIMARY'):
            fld_name = line.split()[0]
            fld_type = line.split()[1].strip(',')
            tbl_data[ks]['table'][tbl]['field'][fld_name]=fld_type
            tbl_data[ks]['table'][tbl]['pk'] = [fld_name]
            tbl_data[ks]['table'][tbl]['cc'] = []
        tbl_data[ks]['table'][tbl]['cql'] += ' ' + line.strip()
      elif line.strip() != ');':
        try:
          tbl_data[ks]['table'][tbl]['cql'] += ' ' + line
          if('AND ' not in line and ' WITH ' not in line):
            fld_name = line.split()[0]
            fld_type = line.replace(fld_name + ' ','').strip(',')
            if (fld_name!='CREATE'):
              tbl_data[ks]['table'][tbl]['field'][fld_name]=fld_type
        except:
          print(('Error1:' + ks + '.' + tbl + ' - ' + line))

# Add tables to be migrated
for ks, ksData in list(tbl_data.items()):
  if (migrate_ks == '' or migrate_ks == ks):
    for tbl, tblData in list(ksData['table'].items()):
      if tblData['type'] == 'Table':
        if len(target_table)>0:
          if (tbl in target_table):
            migrate_tbl_data[tbl]=tbl_data[ks]['table'][tbl]
        if len(target_field_type)>0:
          for field, fieldType in list(tblData['field'].items()):
            if fieldType in target_field_type:
              try:
                type(migrate_tbl_data[tbl])
              except:
                migrate_tbl_data[tbl]=tbl_data[ks]['table'][tbl]
        
#exit(migrate_tbl_data)


for tbl,tblData in list(migrate_tbl_data.items()):

  # Retrieve config file values
  if path.isfile(dm_path + cf_file):
    defaultsFile = open(dm_path + cf_file, 'r')

  # reset config data
  cfg_array = cfg_base_array
  for line in defaultsFile:
    line = line.strip('\n').strip()
    if len(line.split()) > 1:
      cfg_array[line.split()[0]] = line.split()[1]

  defaultsFile.close()
  config_file_data = ''
  fieldData = {}

  # add table elements
  cfg_array['keyspace_table'] = migrate_ks + "." + tbl
  
  # add field elements
  # primary key(s)
  if len(tblData['pk'])>1: cfg_array['fields'] = '('
  else: cfg_array['fields'] = ''
  cfg_array['field_types'] = ''
  first_field = 1
  for key in tblData['pk']:
    if first_field == 1:
      first_field = 0
      cfg_array['partition_keys'] = key
    else:
      cfg_array['fields'] += ','
      cfg_array['field_types'] += ','
      cfg_array['partition_keys'] += ','+key
    process_field(tbl,key,tblData['field'][key],tblData['cql'])
    if len(tblData['pk'])>1: cfg_array['fields'] += '),('
    cfg_array['fields'] += ','
    if len(tblData['cc'])>1: cfg_array['fields'] += '('
  # clustering column(s)
  first_field = 1
  for key in tblData['cc']:
    if first_field == 1:
      first_field = 0
      cfg_array['field_types'] += ','
    else:
      cfg_array['fields'] += ','
      cfg_array['field_types'] += ','
    process_field(tbl,key,tblData['field'][key],tblData['cql'])
    if len(tblData['cc'])>1 : cfg_array['fields'] += ')'

  # non-primary field(s)
  for fieldName,fieldType in list(tblData['field'].items()):
    if fieldName not in tblData['pk'] and fieldName not in tblData['cc']:
      cfg_array['fields'] += ','
      cfg_array['field_types'] += ','
      process_field(tbl,fieldName,fieldType,tblData['cql'])

  # Retrieve migration config template
  if path.isfile(dm_path + template_file):
    templateFile = open(dm_path + template_file, 'r')

  # create table spark migration config file
  for line in templateFile:
    newline = line
    if '<<' in line and '>>' in line:
      cfg_param = (line.split('<<'))[1].split('>>')[0]
      if cfg_param in cfg_array.keys():
        newline = line.replace('<<'+cfg_param+'>>',cfg_array[cfg_param])
      
    config_file_data += newline
  
  templateFile.close()

  # add sql reference
  config_file_data += '\n/* CQL Reference:\n' + tblData['cql'] + '\n/*'

  # Create Spark Migration Table Config File
  cfgFile = open(dm_path+migrate_ks+'_'+tbl+'_SparkConfig.properties', 'w')
  cfgFile.write(config_file_data)
  print('Migration Config File Created: '+dm_path+migrate_ks+'_'+tbl+'_SparkConfig.properties')
  cfgFile.close()
