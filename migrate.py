# Project Description: Migrate data from Google Sheets to Snowflake
# Author: Monty Dimkpa
# Version: 1.0
# Inquiries: cmdimkpa@gmail.com

# import required libraries
import os
import json
import sys
import snowflake.connector
import gspread
from time import sleep

RATE_LIMIT_DELAY_SECS = 3

# determine script location and adjust for Operating System
THIS_DIR = os.getcwd()  
if "\\" in THIS_DIR:
    slash = "\\"
else:
    slash = "/"
THIS_DIR+=slash

# your local snowflake credentials file
snowflake_creds_file = THIS_DIR+"snowflake.json"

# your local Google Sheets API credentials file
googlesheets_creds_file = THIS_DIR+"googlesheets.json"

# extract required runtime arguments from CLI
args = sys.argv

# Google Sheets arguments
try:
    googlesheets_target_sheet = args[1]
except:
    print("Error: please provide GOOGLESHEETS_TARGET_SHEET as 1st argument")
    sys.exit()
try:
    googlesheets_read_cols = [x for x in args[2].split(",") if x]
except:
    print("Error: please provide GOOGLESHEETS_READ_COLS as 2nd argument")
    sys.exit()
try:
    googlesheets_max_row = int(float(args[3]))
except:
    print("Error: please provide GOOGLESHEETS_MAX_ROW as 3rd argument")
    sys.exit()

# Snowflake arguments
try:
    snowflake_target_warehouse = args[4]
except:
    print("Error: please provide SNOWFLAKE_TARGET_WAREHOUSE as 4th argument")
    sys.exit()
try:
    snowflake_target_database = args[5]
except:
    print("Error: please provide SNOWFLAKE_TARGET_DATABASE as 5th argument")
    sys.exit()
try:
    snowflake_target_table = args[6]
except:
    print("Error: please provide SNOWFLAKE_TARGET_TABLE as 6th argument")
    sys.exit()
try:
    snowflake_target_schema = args[7]
except:
    print("Error: please provide SNOWFLAKE_TARGET_SCHEMA as 7th argument")
    sys.exit()
try:
    snowflake_target_field_names = [x for x in args[8].split(",") if x]
except:
    print("Error: please provide SNOWFLAKE_TARGET_FIELD_NAMES as 8th argument")
    sys.exit()
try:
    snowflake_target_role = args[9]
except:
    print("Error: please provide SNOWFLAKE_TARGET_ROLE as 9th argument")
    sys.exit()

def read_json_file(file):
    process = open(file,"rb+")
    creds_str = process.read()
    process.close()
    return json.loads(creds_str)

def apply_type(x):
    try:
        test = float(x)
        return x
    except:
        return "'%s'" % x

# pull your Snowflake credentials; create connection and cursor object
credsSF = read_json_file(snowflake_creds_file)
ctx = snowflake.connector.connect(
    user=credsSF["user"],
    password=credsSF["password"],
    account=credsSF["account"],
    warehouse=snowflake_target_warehouse,
    database=snowflake_target_database,
    schema=snowflake_target_schema
)
cs = ctx.cursor()

# connect to your Google Sheet
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]
client = gspread.service_account(filename='googlesheets.json')
GoogleSheet = client.open(googlesheets_target_sheet).sheet1

# your events log
eventlog = THIS_DIR+"events.log"

class EventManager:
    def __init__(self):
        try:
            self.eventlogProcess = open(eventlog, "rb+")
            self.realtimeLog = json.loads(self.eventlogProcess.read())
            self.eventlogProcess.close()
        except:
            self.realtimeLog = {
                "onRow": 0,
                "columnList": [],
                "fieldNames":[],
                "sql": []
            }
        self.sheetObject = None
        self.readLimit = None
        self.targetCursor = None
        self.targetTable = None
        self.SQLInsertTemplate = 'INSERT INTO %s (%s) VALUES (%s)'
        self.eventlogProcess = None

    def update_target_table(self, table):
        self.targetTable = table
    
    def update_target_role(self, role):
        if self.targetCursor:
            self.targetCursor.execute("USE ROLE %s" % role)

    def update_sheet_object(self, sheet):
        self.sheetObject = sheet

    def update_target_cursor(self, cursor):
        self.targetCursor = cursor

    def update_read_cols(self, new_cols):
        self.realtimeLog["columnList"] = new_cols

    def update_field_names(self, names):
        self.realtimeLog["fieldNames"] = names

    def update_read_limit(self, max_row):
        self.readLimit = max_row

    def log(self):
        self.eventlogProcess = open(eventlog, "wb+")
        self.eventlogProcess.write(json.dumps(self.realtimeLog).encode())
        self.eventlogProcess.close()

    def run(self):
        if self.targetTable and self.sheetObject and self.readLimit and self.targetCursor and self.realtimeLog["columnList"] and self.realtimeLog["fieldNames"]:
            writes = 0
            EOF = False
            while self.readLimit > self.realtimeLog["onRow"] and not EOF:
                self.realtimeLog["onRow"]+=1
                values = ",".join([apply_type(self.sheetObject.acell("%s%s" % (
                    column, self.realtimeLog["onRow"])).value) for column in self.realtimeLog["columnList"]])
                sql = self.SQLInsertTemplate % ("%s.%s.%s" % (snowflake_target_database, snowflake_target_schema, self.targetTable), ",".join(self.realtimeLog["fieldNames"]), values)
                if 'None' in sql:
                    EOF = True
                else:
                    self.targetCursor.execute(sql)
                    self.realtimeLog["sql"].append(sql)
                    self.log()
                    writes+=1
                    print("200 OK on Write #%s - Query: %s" % (writes, sql))
                    sleep(RATE_LIMIT_DELAY_SECS)
            self.shutdown()
        else:
            print("Error: please provide required command line arguments to begin")
            sys.exit()

    def shutdown(self):
        self.log()

# create and update migration event manager
currentEvents = EventManager()
currentEvents.update_sheet_object(GoogleSheet)
currentEvents.update_target_cursor(cs)
currentEvents.update_target_table(snowflake_target_table)
currentEvents.update_read_cols(googlesheets_read_cols)
currentEvents.update_field_names(snowflake_target_field_names)
currentEvents.update_read_limit(googlesheets_max_row)
currentEvents.update_target_role(snowflake_target_role)

# run event manager
currentEvents.run()

# session finished; close any open connections and exit
ctx.close()
sys.exit()
