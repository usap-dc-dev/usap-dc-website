#!/usr/bin/python3
import os


for year in range(2018, 2023):
    for month in range(1, 13):
        if month < 10: 
            month_str = '0%s' % month
        else:
            month_str = '%s' % month

        try:
            os.system("sudo /opt/rh/python27/root/usr/bin/python readAccessLogs_with_search.py %s %s" % (year, month_str))
        except:
            continue
        
        
