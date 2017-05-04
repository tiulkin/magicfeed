#!/usr/bin/env python
import couchdb
import subprocess
import csv
import time
import sys
from dateutil.parser import *
from constants import *

import glob

# import StringIO

server_url = "http://" + COUCH_CHANNELS_CREDS["creds"] + "@" + COUCH_SERVERS[DEFAULT_USERS_SERVER]["host"]
server = couchdb.client.Server(server_url)
database = server["nginx"]


def read_file(file_name):
    cmd = ["zgrep", "_changes?feed=", file_name]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=False)
    temp = process.communicate()[0].decode(encoding='UTF-8')
    logreader = csv.reader(temp.splitlines(), delimiter=' ')  # StringIO.StringIO(temp) #, delimiter=' ')
    return (logreader)


def import_log():
    tmpfiles = glob.glob('/var/log/nginx/access.*')
    tmpfiles.sort()
    files = tmpfiles[1:]
    files.append(tmpfiles[0])
    files.reverse()
    last_log = database.view("logs/logByTime", descending=True, limit=1, reduce=False)
    last_time = 0
    for record in last_log:
        last_time = int(record.key)
        break

    fileindex = 0
    for logfile in files:
        log_time = 0
        logreader = read_file(logfile)
        fileindex += 1
        for row in logreader:
            log_time = time.mktime(parse(row[3][1:-9] + " " + row[3][-8:] + " " + row[4][:-1]).timetuple()) * 1000
            break
        if log_time < last_time:
            break

    while fileindex >= 0:
        fileindex -= 1
        result = []
        logfile = files[fileindex]
        logreader = read_file(logfile)
        for row in logreader:
            if row:
                index1 = row[5].find("_changes?feed")
                channel_id = row[5][9:index1 - 1]
                is_mobile = True if "_m" in channel_id else False
                log_time_tuple = parse(row[3][1:-9] + " " + row[3][-8:] + " " + row[4][:-1]).timetuple()
                log_time = time.mktime(log_time_tuple) * 1000
                if channel_id and log_time > last_time:
                    result.append({
                        "ip": row[0],
                        "year": log_time_tuple[0],
                        "month": log_time_tuple[1],
                        "day": log_time_tuple[2],
                        "hour": log_time_tuple[3],
                        "minute": log_time_tuple[4],
                        "time": log_time,
                        "cannelId": channel_id if not is_mobile else channel_id[:-2],
                        "isMobile": is_mobile
                    })
        if len(result):
            database.save({"_id": str(int(time.time() * 1000)), "logrecords": result})


import_log()