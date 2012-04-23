'''
Author: Yaling Zheng
Jan 2012
Holland Computing Center, University of Nebraska-Lincoln 

This script produces a xrootd report in the following format within the last 24 hours
between yesterday 14:00:00 GMT and today 14:00:00 GMT

All sites

Overflow: 314: (1.77% wall 0.83%) Normal:17435
Exit 0: 76.75% (vs 62.89%) wall 70.30%
Exit 84: 0.32% (vs 0.52%) wall 0.02%
Efficiency: 77.02% (vs 85.27%)
Eff  >80%: 62.74% (vs 54.38%)

Only UCSD+Nebraska+Wisconsin+Purdue

Overflow: 314: (1.78% wall 0.83%) Normal:17355
Exit 0: 76.75% (vs 62.74%) wall 70.30%
Exit 84: 0.32% (vs 0.52%) wall 0.02%
Efficiency: 77.02% (vs 85.27%)
Eff  >80%: 62.74% (vs 54.64%)

Possible Overflow Jobs with Exit Code 84 based on xrootd log

for cmssrv32.fnal.gov:1094:
    for /CN=Nicholas S Eggert 114717:
        408235.127, 2012-04-05 20:03:15 GMT--2012-04-05 20:13:20 GMT,
          /store/mc/Fall11/WJetsToLNu_TuneZ2_7TeV-madgraph-tauola/AODSIM/PU_S6_START42_V14B-v1/0000/1EEE763D-1AF2-E011-8355-00304867D446.root

'''

import os
from sets import Set
import re
import time
from time import gmtime, strftime
from datetime import datetime, date, timedelta;
from pytz import timezone
import MySQLdb
import ConfigParser
import string


now = date.today();

yesterday = date.today() - timedelta(1);

'''
Job Latest End Time
this is the GMT time that UCSD glidein-2 mailer@glidein-2.t2.ucsd.edu
central time 02:00:00 correponds to pacific time 00:00:00, which is the time that UCSD generates 
report

'''
import pytz, datetime

import os
import time

os.environ['TZ'] = "US/Pacific"
time.tzset()
UCSDoffset = time.timezone/3600

os.environ['TZ'] = "US/Central"
time.tzset()
Nebraskaoffset = time.timezone/3600

strftimestring = "%%Y-%%m-%%d %d:00:00"

if (UCSDoffset>=10):
    UCSDtimestring = "%%Y-%%m-%%d %d:00:00" % UCSDoffset
else:
    UCSDtimestring = "%%Y-%%m-%%d 0%d:00:00" % UCSDoffset
#print UCSDtimestring

LatestEndTime = now.strftime(UCSDtimestring)
EarliestEndTime = yesterday.strftime(UCSDtimestring)

#print LatestEndTime
#print EarliestEndTime

def ConnectDatabase():
    '''
    Connect to rcf-gratia.unl.edu, and prepare database gratia for querying
    '''
    # read configuration file, get username and password of one user
    config = ConfigParser.ConfigParser()
    config.read("mygratiaDBpwd.ini")
    username = config.get("rcf-gratia", "username")
    password = config.get("rcf-gratia", "password")
    # connect with the database
    db = MySQLdb.connect("rcf-gratia.unl.edu", username, password, "gratia", 49152)

    # prepare a cursor oject using cursor() method
    cursor = db.cursor()
    
    # return database cursor, and job latest End time and earliest end time
    return db, cursor


def QueryGratia(cursor):

    '''
    Query database gratia, and compute the following:
    for all sites (and sites in the form of %UCSD% %Purdue% %Nebraska% %GLOW% (Grid Laboratory of Wisconsin))
    (1) the number of overflow jobs, the percentage of number of overflow jobs OVER number of all jobs,
    the percentage of walltime of overflow jobs OVER walltime of all jobs, the number of normal jobs
    (2) the percentage of number of overflow jobs with exit code 0 OVER number of overflow jobs, 
    the percentage of number of normal jobs with exit code 0 OVER number of normal jobs,
    the percentage of walltime of overflow jobs with exit code 0 OVER walltime of overflow jobs 
    (3) the percentage of number of overflow jobs with exit code 84 OVER number of overflow jobs, 
    the percentage of number of normal jobs with exit code 84 OVER number of normal jobs,
    the percentage of walltime of overflow jobs with exit code 84 OVER walltime of overflow jobs 
    (4) the efficiency of overflow jobs, which is equal to (CpuUserDuration+CpuSystemDuration)/WallDuration,
    the efficiency of normal jobs
    (5) the percentage of number of overflow jobs whose efficiency is greater than 80%, 
    the percentage of number of normal jobs whose efficiency is greater than 80%
    '''
    
    # Compute number (wallduration, CpuUserDuration+CpuSystemDuration) of overflow jobs in all sites
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration), SUM(CpuUserDuration+CpuSystemDuration)
    from JobUsageRecord
    where
      EndTime>="%s" and EndTime<"%s"
      AND HostDescription like '%%-overflow'
      AND ResourceType="BatchPilot";
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone();
    NumOverflowJobs = int(row[0])
    if (NumOverflowJobs==0):
        WallDurationOverflowJobs = 0
        UserAndSystemDurationOverflowJobs = 0
    else:
        WallDurationOverflowJobs = float(row[1])
        UserAndSystemDurationOverflowJobs = float(row[2])

    # Compute efficiency of overflow jobs, which is equal to (CpuUserDuration+CpuSystemDuration)/WallDuration (in all sites)
    if (WallDurationOverflowJobs==0):
        EfficiencyOverflowJobs = 0
    else:
        EfficiencyOverflowJobs = float(100* UserAndSystemDurationOverflowJobs)/WallDurationOverflowJobs;
    
    # compute number (wallduration, CpuUserDuration+CpuSystemDuration) of normal jobs (in all sites)
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration), SUM(CpuUserDuration+CpuSystemDuration)
    from JobUsageRecord
    where
      EndTime>="%s" and EndTime<"%s"
      AND HostDescription NOT like '%%-overflow'
      AND ResourceType="BatchPilot";
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone();
    NumNormalJobs = int(row[0])
    NumAllJobs = NumOverflowJobs + NumNormalJobs
    if (NumNormalJobs == 0):
        WallDurationNormalJobs = 0
        UserAndSystemDurationNormalJobs = 0
    else:
        WallDurationNormalJobs = float(row[1])  
        UserAndSystemDurationNormalJobs = float(row[2])
    WallDurationAllJobs = WallDurationNormalJobs + WallDurationNormalJobs
    #print NumNormalJobs 

    # Compute the efficiency of normal jobs (in all sites)
    if (WallDurationNormalJobs == 0):
        EfficiencyNormalJobs = 0
    else:
        EfficiencyNormalJobs = float(100*UserAndSystemDurationNormalJobs)/WallDurationNormalJobs

    # Compute the percentage of number of overflow jobs OVER number of all jobs (in all sites)
    if (NumAllJobs==0):
        PercentageOverflowJobs = 0
    else:
        PercentageOverflowJobs = float(NumOverflowJobs*100)/NumAllJobs;
    #print str(PercentageOverflowJobs)+"%"
 
    # Compute the percentage of walltime of overflow jobs OVER walltime of all jobs (in all sites)
    if (WallDurationAllJobs==0):
        PercentageWallDurationOverflowJobs = 0
    else:
        PercentageWallDurationOverflowJobs = float(WallDurationOverflowJobs*100)/WallDurationAllJobs;
    #print str(PercentageWallDurationOverflowJobs)+"%"

    # Compute the percentage of number (wallduration) of overflow jobs with exit code 0 OVER number of overflow jobs (in all sites)
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration)
    from JobUsageRecord JUR 
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    where
      EndTime>="%s" and EndTime<"%s"
      AND RESC.value = 0
      AND HostDescription like '%%-overflow'
      AND ResourceType="BatchPilot";
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumOverflowJobsExitCode0 = int(row[0])
    if (NumOverflowJobs==0):
        PercentageExitCode0Overflow = 0
    else:
        PercentageExitCode0Overflow =float(100*NumOverflowJobsExitCode0)/ NumOverflowJobs
    #print str(PercentageExitCode0Overflow) + "%"
    if (NumOverflowJobsExitCode0==0):
        WallDurationOverflowJobsExitCode0 = 0
    else:
        WallDurationOverflowJobsExitCode0 = float(row[1]) 

    # Compute the walltime percentage of walltime of overflow jobs with exit code 0 OVER walltime of overflow jobs (in all sites)
    if (WallDurationOverflowJobs==0):
        PercentageWallDurationOverflowJobsExitCode0 = 0
    else:
        PercentageWallDurationOverflowJobsExitCode0 = float(100*WallDurationOverflowJobsExitCode0)/WallDurationOverflowJobs;
  
    # Compute the percentage of number normal jobs with exit code 0 OVER number of normal jobs (in all sites)
    querystring = """
    SELECT
        COUNT(*)
    from JobUsageRecord JUR 
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    where
      EndTime>="%s" and EndTime<"%s"
      AND RESC.value = 0
      AND HostDescription NOT like '%%-overflow'
      AND ResourceType="BatchPilot";
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumNormalJobsExitCode0 = int(row[0])
    if (NumNormalJobs==0):
        PercentageExitCode0Normal = 0
    else:
        PercentageExitCode0Normal = float(100*NumNormalJobsExitCode0)/NumNormalJobs
    #print str(PercentageExitCode0Normal) + "%"
    
    # compute number of overflow jobs with exit code 84 (in all sites)
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration)
    from JobUsageRecord JUR 
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    where
      EndTime>="%s" and EndTime<"%s"
      AND RESC.value = 84
      AND HostDescription like '%%-overflow'
      AND ResourceType="BatchPilot";
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumOverflowJobsExitCode84 = int(row[0])

    # Compute the percentage of number of overflow jobs with exit code 84 OVER number of overflow jobs (in all sites)
    if (NumOverflowJobs==0):
        PercentageNumOverflowJobsExitCode84 = 0
    else:
        PercentageNumOverflowJobsExitCode84 = float(100*NumOverflowJobsExitCode84)/NumOverflowJobs;
    #print str(PercentageNumOverflowJobsExitCode84)+"%"

    # compute walltime of overflow jobs with exit code 84 (in all sites)
    if (NumOverflowJobsExitCode84==0):
        WallDurationOverflowJobsExitCode84 = 0
    else:
        WallDurationOverflowJobsExitCode84 = float(row[1])
    #print str(WallDurationOverflowJobsExitCode84)

    # Compute the percentage of walltime of overflow jobs with exit code 84 OVER walltime of overflow jobs (in all sites) 
    PercentageWallDurationOverflowJobsExitCode84 = float(100*WallDurationOverflowJobsExitCode84)/WallDurationOverflowJobs;     
    #print str(PercentageWallDurationOverflowJobsExitCode84)+"%"

    # compute number of normal jobs with exit code 84 (in all sites)
    querystring = """
    SELECT
        COUNT(*)
    from JobUsageRecord JUR 
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    where
      EndTime>="%s" and EndTime<"%s"
      AND RESC.value = 84
      AND HostDescription NOT like '%%-overflow'
      AND ResourceType="BatchPilot";
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    NumNormalJobsExitCode84 = int(row[0])

    # Compute the percentage of number of normal jobs with exit code 84 OVER number of normal jobs (in all sites) 
    if (NumNormalJobs==0):
        PercentageNumNormalJobsExitCode84 = 0
    else:
        PercentageNumNormalJobsExitCode84 = float(100*NumNormalJobsExitCode84)/NumNormalJobs
    #print str(PercentageNumNormalJobsExitCode84)+"%"

    # Compute the percentage of number of overflow jobs whose efficiency greater than 80% (in all sites)
    querystring = """
    SELECT
        COUNT(*)
    from JobUsageRecord 
    where
      EndTime>="%s" and EndTime<"%s"
      AND HostDescription like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (CpuUserDuration+CpuSystemDuration)/WallDuration > 0.8;
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumEfficiencyGT80percentOverflowJobs = int(row[0])
    if (NumOverflowJobs == 0):
        PercentageEfficiencyGT80percentOverflowJobs = 0
    else:
        PercentageEfficiencyGT80percentOverflowJobs = float(100*NumEfficiencyGT80percentOverflowJobs)/NumOverflowJobs
    #print str(PercentageEfficiencyGT80percentOverflowJobs) + "%"

    # Compute the percentage of number of normal jobs whose efficiency greater than 80% (in all sites)
    querystring = """
    SELECT
        COUNT(*)
    from JobUsageRecord  
    where
      EndTime>="%s" and EndTime<"%s"
      AND HostDescription NOT like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (CpuUserDuration+CpuSystemDuration)/WallDuration > 0.8;
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumEfficiencyGT80percentNormalJobs = int(row[0])
    if (NumNormalJobs == 0):
        PercentageEfficiencyGT80percentNormalJobs = 0
    else:
        PercentageEfficiencyGT80percentNormalJobs = float(100*NumEfficiencyGT80percentNormalJobs)/NumNormalJobs
    #print str(PercentageEfficiencyGT80percentNormalJobs) + "%"
 
    # Compute the number (walltime) of all jobs in 4 sites
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration), SUM(CpuUserDuration+CpuSystemDuration)
    from JobUsageRecord JUR 
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>="%s" and EndTime<"%s"
      AND ResourceType="BatchPilot"
      AND (SiteName like '%%Nebraska%%' or SiteName like '%%UCSD%%' or SiteName like '%%Purdue%%' or SiteName like '%%GLOW%%');
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumAllJobs4sites = int(row[0])
    if (NumAllJobs4sites==0):
        WallDurationAllJobs4sites = 0
    else:
        WallDurationAllJobs4sites = float(row[1])

    # Compute the number of overflow jobs that in %UCSD%, %Nebraska%, %GLOW%, and %Purdue%
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration), SUM(CpuUserDuration+CpuSystemDuration)
    from JobUsageRecord JUR 
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>="%s" and EndTime<"%s"
      AND HostDescription like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (SiteName like '%%Nebraska%%' or SiteName like '%%UCSD%%' or SiteName like '%%Purdue%%' or SiteName like '%%GLOW%%');
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumOverflowJobs4sites = int(row[0])
    if (NumOverflowJobs4sites==0):
        WallDurationOverflowJobs4sites = 0
        UserAndSystemDurationOverflowJobs4sites = 0
    else:
        WallDurationOverflowJobs4sites = float(row[1])
        UserAndSystemDurationOverflowJobs4sites = float(row[2])

    # Compute the percentage of walltime of overflow jobs OVER walltime of all jobs (in 4 sites)

    if (WallDurationAllJobs4sites == 0):
        PercentageWallDurationOverflowJobs4sites = 0
    else:
        PercentageWallDurationOverflowJobs4sites = float(100*WallDurationOverflowJobs4sites)/WallDurationAllJobs4sites
    #print str(PercentageWallDurationOverflowJobs4sites)+"%"

    # Compute the efficiency of overflow jobs (in 4 sites)
    if (WallDurationOverflowJobs4sites == 0):
        EfficiencyOverflowJobs4sites = 0
    else:
        EfficiencyOverflowJobs4sites = float(100* UserAndSystemDurationOverflowJobs4sites)/WallDurationOverflowJobs4sites

    #print str(NumOverflowJobs4sites)
    
    # Compute the number (efficiency) of normal jobs (in 4 sites)

    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration), SUM(CpuUserDuration+CpuSystemDuration)
    from JobUsageRecord JUR
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>="%s" and EndTime<"%s"
      AND HostDescription NOT like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (SiteName like '%%Nebraska%%' or SiteName like '%%UCSD%%' or SiteName like '%%Purdue%%' or SiteName like '%%GLOW%%');
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumNormalJobs4sites = int(row[0])
    #print str(NumNormalJobs4sites)
    if (NumNormalJobs4sites == 0):
        UserAndSystemDurationNormalJobs4sites = 0
        WallDurationNormalJobs4sites = 0
    else:
        UserAndSystemDurationNormalJobs4sites = float(row[2])
        WallDurationNormalJobs4sites = float(row[1])
    if (WallDurationNormalJobs4sites == 0):
        EfficiencyNormalJobs4sites = 0
    else:
        EfficiencyNormalJobs4sites = float(100*UserAndSystemDurationNormalJobs4sites)/WallDurationNormalJobs4sites
    #print str(EfficiencyNormalJobs4sites) + "%"
  
    # Compute the percentage of number of overflow jobs OVER number of all jobs (in 4 sites)
    if (NumAllJobs4sites == 0):
        PercentageOverflowJobs4sites = 0
    else:
        PercentageOverflowJobs4sites = float(100*NumOverflowJobs4sites)/NumAllJobs4sites;
    #print str(PercentageOverflowJobs4sites) + "%"
   
    # Compute the number of overflow jobs with exit code 0 (in 4 sites)
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration)
    from JobUsageRecord JUR 
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>="%s" and EndTime<"%s"
      AND RESC.value = 0
      AND HostDescription like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (SiteName like '%%Nebraska%%' or SiteName like '%%UCSD%%' or SiteName like '%%Purdue%%' or SiteName like '%%GLOW%%');
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumOverflowJobsExitCode0foursites = int(row[0])

    # Compute the percentage of number of overflow jobs with exit code 0 OVER number of overflow jobs (in 4 sites)
    if (NumOverflowJobs4sites == 0):
        PercentageOverflowJobsExitCode0foursites = 0
    else:
        PercentageOverflowJobsExitCode0foursites = float(100*NumOverflowJobsExitCode0foursites)/NumOverflowJobs4sites
    #print str(PercentageOverflowJobsExitCode0foursites)+"%"
    if (NumOverflowJobsExitCode0foursites == 0):
        WallDurationOverflowJobsExitCode0foursites = 0
    else:
        WallDurationOverflowJobsExitCode0foursites = int(row[1])

    # Compute the percentage of walltime of overflow jobs with exit code 0 OVERFLOW the walltime of all overflow jobs (in 4 sites)
    if (WallDurationOverflowJobs4sites == 0):
        PercentageWallDurationOverflowJobsExitCode0foursites = 0
    else:
        PercentageWallDurationOverflowJobsExitCode0foursites = float(100*WallDurationOverflowJobsExitCode0foursites)/WallDurationOverflowJobs4sites

    # Compute the number of normal jobs with exit code 0 (in 4 sites)
    querystring = """
    SELECT
        COUNT(*)
    from JobUsageRecord JUR 
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>="%s" and EndTime<"%s"
      AND RESC.value = 0
      AND HostDescription NOT like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (SiteName like '%%Nebraska%%' or SiteName like '%%UCSD%%' or SiteName like '%%Purdue%%' or SiteName like '%%GLOW%%');
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumNormalJobsExitCode0foursites = int(row[0])

    # Compute the percentage of number of normal jobs with exit code 0 OVER number of normal jobs (in 4 sites)
    if (NumNormalJobs4sites == 0):
        PercentageNormalJobsExitCode0foursites = 0
    else:
        PercentageNormalJobsExitCode0foursites = float(100*NumNormalJobsExitCode0foursites)/NumNormalJobs4sites
    #print str(PercentageNormalJobsExitCode0foursites)+"%"

    # Compute number of overflow jobs with exit code 84 (in 4 sites)
    querystring = """
    SELECT
        COUNT(*), SUM(WallDuration)
    from JobUsageRecord JUR 
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>="%s" and EndTime<"%s"
      AND RESC.value = 84
      AND HostDescription like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (SiteName like '%%Nebraska%%' or SiteName like '%%UCSD%%' or SiteName like '%%Purdue%%' or SiteName like '%%GLOW%%');
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumOverflowJobsExitCode84foursites = int(row[0])

    # Compute percentage of number of overflow jobs with exit code 84 OVER number of overflow jobs (in 4 sites)
    PercentageOverflowJobsExitCode84foursites = float(100*NumOverflowJobsExitCode84foursites)/NumOverflowJobs4sites
    #print str(PercentageOverflowJobsExitCode84foursites)+"%"

    if (NumOverflowJobsExitCode84foursites == 0):
        WallDurationOverflowJobsExitCode84foursites = 0
    else:
        WallDurationOverflowJobsExitCode84foursites = float(row[1])

    # Compute the percentage of walltime of overflow jobs with exit code 84 OVER walltime of overflow jobs (in 4 sites) 
    if (WallDurationOverflowJobs4sites == 0):
        PercentageWallDurationOverflowJobsExitCode84foursites  = 0
    else:
        PercentageWallDurationOverflowJobsExitCode84foursites = float(100*WallDurationOverflowJobsExitCode84foursites)/WallDurationOverflowJobs4sites
    #print str(PercentageWallDurationOverflowJobsExitCode84foursites)+"%"

    # Compute number of normal jobs with exit code 84 (in 4 sites)
    querystring = """
    SELECT
        COUNT(*)
    from JobUsageRecord JUR 
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>="%s" and EndTime<"%s"
      AND RESC.value = 84
      AND HostDescription NOT like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (SiteName like '%%Nebraska%%' or SiteName like '%%UCSD%%' or SiteName like '%%Purdue%%' or SiteName like '%%GLOW%%');
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumNormalJobsExitCode84foursites = int(row[0])

    # Compute percentage of number of normal jobs with exit code 84 OVER number of normal jobs (in 4 sites)
    if (NumNormalJobs4sites == 0):
        PercentageNormalJobsExitCode84foursites = 0
    else:
        PercentageNormalJobsExitCode84foursites = float(100*NumNormalJobsExitCode84foursites)/NumNormalJobs4sites
    #print str(PercentageNormalJobsExitCode84foursites)+"%"

    # Compute the percentage of number of overflow jobs whose efficiency greater than 80% (in 4 sites)
    querystring = """
    SELECT
        COUNT(*)
    from JobUsageRecord JUR 
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>="%s" and EndTime<"%s"
      AND HostDescription like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (SiteName like '%%Nebraska%%' or SiteName like '%%UCSD%%' or SiteName like '%%Purdue%%' or SiteName like '%%GLOW%%')
      AND (CpuUserDuration+CpuSystemDuration)/WallDuration>0.8; 
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumEfficiencyGT80percentOverflowJobs4sites = int(row[0])
    if (NumOverflowJobs4sites == 0):
        PercentageEfficiencyGT80percentOverflowJobs4sites = 0
    else:
        PercentageEfficiencyGT80percentOverflowJobs4sites = float(100*NumEfficiencyGT80percentOverflowJobs4sites)/NumOverflowJobs4sites
    #print str(PercentageEfficiencyGT80percentOverflowJobs4sites) + "%"

    # Compute the percentage of normal jobs whose efficiency greater than 80% (in 4 sites)
    querystring = """
    SELECT
        COUNT(*)
    from JobUsageRecord JUR 
    JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid)
    JOIN Probe P on (JURM.ProbeName = P.probename)
    JOIN Site S on (P.siteid = S.siteid)
    where
      EndTime>="%s" and EndTime<"%s"
      AND HostDescription NOT like '%%-overflow'
      AND ResourceType="BatchPilot"
      AND (SiteName like '%%Nebraska%%' or SiteName like '%%UCSD%%' or SiteName like '%%Purdue%%' or SiteName like '%%GLOW%%')
      AND (CpuUserDuration+CpuSystemDuration)/WallDuration>0.8; 
     """
    cursor.execute(querystring % (EarliestEndTime, LatestEndTime));
    row = cursor.fetchone()
    NumEfficiencyGT80percentNormalJobs4sites = int(row[0])
    if (NumNormalJobs4sites == 0):
        PercentageEfficiencyGT80percentNormalJobs4sites = 0
    else:
        PercentageEfficiencyGT80percentNormalJobs4sites = float(100*NumEfficiencyGT80percentNormalJobs4sites)/NumNormalJobs4sites
    #print str(PercentageEfficiencyGT80percentNormalJobs4sites) + "%"
    
    # Print out the statistics 
    print "\nAll sites\n"

    print "Overflow: %d: (%.2f%s wall %.2f%s) Normal:%d" % (NumOverflowJobs, PercentageOverflowJobs, "%", PercentageWallDurationOverflowJobs, "%", NumNormalJobs)

    print "Exit 0: %.2f%s (vs %.2f%s) wall %.2f%s" % (PercentageExitCode0Overflow, "%", PercentageExitCode0Normal, "%", PercentageWallDurationOverflowJobsExitCode0, "%")

    print "Exit 84: %.2f%s (vs %.2f%s) wall %.2f%s" % (PercentageNumOverflowJobsExitCode84, "%", PercentageNumNormalJobsExitCode84,"%", PercentageWallDurationOverflowJobsExitCode84, "%")

    print "Efficiency: %.2f%s (vs %.2f%s)" % (EfficiencyOverflowJobs, "%", EfficiencyNormalJobs, "%")

    print "Eff  >80%s: %.2f%s (vs %.2f%s)" % ("%", PercentageEfficiencyGT80percentOverflowJobs, "%", PercentageEfficiencyGT80percentNormalJobs, "%")

    print "\nOnly UCSD+Nebraska+Wisconsin+Purdue\n"

    print "Overflow: %d: (%.2f%s wall %.2f%s) Normal:%d" % (NumOverflowJobs4sites, PercentageOverflowJobs4sites, "%", PercentageWallDurationOverflowJobs4sites, "%", NumNormalJobs4sites)

    print "Exit 0: %.2f%s (vs %.2f%s) wall %.2f%s" % (PercentageOverflowJobsExitCode0foursites, "%", PercentageNormalJobsExitCode0foursites, "%", PercentageWallDurationOverflowJobsExitCode0foursites, "%") 

    print "Exit 84: %.2f%s (vs %.2f%s) wall %.2f%s" % (PercentageOverflowJobsExitCode84foursites, "%", PercentageNormalJobsExitCode84foursites, "%", PercentageWallDurationOverflowJobsExitCode84foursites, "%")

    print "Efficiency: %.2f%s (vs %.2f%s)" % (EfficiencyOverflowJobs4sites, "%", EfficiencyNormalJobs4sites, "%")

    print "Eff  >80%s: %.2f%s (vs %.2f%s)" % ("%", PercentageEfficiencyGT80percentOverflowJobs4sites, "%", PercentageEfficiencyGT80percentNormalJobs4sites, "%")


def FilterCondorJobs(cursor):

    '''
    For each overflow job with exit code 84, we check possible correponding job in xrootd log
    and output the job in the following format
    for cmssrv32.fnal.gov:1094:
        for /CN=Nicholas S Eggert 114717:
          408235.127, 2012-04-05 20:03:15 GMT--2012-04-05 20:13:20 GMT,
           /store/mc/Fall11/WJetsToLNu_TuneZ2_7TeV-madgraph-tauola/AODSIM/PU_S6_START42_V14B-v1/0000/1EEE763D-1AF2-E011-8355-00304867D446.root
    '''
    # Find those overflow jobs whose exit code is 84 and resource type is BatchPilot 
    querystring = """
    SELECT JUR.dbid, LocalJobId, CommonName, Host, StartTime, EndTime
    from JobUsageRecord JUR
    JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description="ExitCode"))
    JOIN JobUsageRecord_Meta JURM on JUR.dbid = JURM.dbid
    where 
      EndTime >= "%s" AND EndTime < "%s"
      AND ResourceType = "BatchPilot"
      AND RESC.value = 84
      AND HostDescription like '%%-overflow';
    """
    cursor.execute(querystring %(EarliestEndTime, LatestEndTime));
    # Handle each record
    numrows = int(cursor.rowcount)
    print "\nPossible Overflow Jobs with Exit Code 84 based on xrootd log\n"
    for i in range(numrows):
        row = cursor.fetchone()
        #print row[0], row[1], row[2], row[3], row[4], row[5]
        localjobid = row[1]
        commonname = row[2]
        host = row[3]
        starttime = row[4]
        endtime = row[5]
	gmstarttime = starttime
	gmendtime = endtime
	#print host
	if (host!="NULL"):
            # Check each job in xrootd log
            matchedflag = CheckJobMatchInXrootdLog(localjobid, commonname, host, starttime, endtime, gmstarttime, gmendtime)


def CheckJobMatchInXrootdLog(localjobid, commonname, host, starttime, endtime, gmstarttime, gmendtime):
    '''
    Below is the format of analysis of possible overflow xrootd jobs with exitcode 84 in the following format
    for cmssrv32.fnal.gov (a redirection site)
       for user .../.../CN=Brian (a x509UserProxyVOName)
          locajobid starttime endtime jobfilename 

    Brian and I guess the overflow jobs as follows. First, we search
    the gratia database the overflow jobs with exit code 84. Then, for
    each such job J, we refer the xrootd.unl.edu log file, and find
    corresponding xrootd.unl.edu records by guessing: if xrootd log
    show that there is a job whose host machine matches this job's
    host machine, and whose login time is within 10 minutes of job J's
    start time and whose disconnection time is within 10 minutes of
    job J's disconnection time, then this job is a possible xrootd
    overflow job.

    We check our corresponding xrootd log, and see whether we can
    track the activity of this job.  We check 2 dictionaries:
    jobLoginDisconnectionAndSoOnDictionary and hostnameJobsDictionary,
    and see whether there exist jobs that satisfying the the
    requirement
    '''
    
    hostnameitems = host.split(" ")
    hostname = hostnameitems[0]
    # print hostname
    possiblejobs = hostnameJobsDictionary.get(hostname, None)
    if (not possiblejobs):
        # we try to fetch the
        hostnameitems = hostname.split(".")
        abbHostname = hostnameitems[0]
        possiblejobs = hostnameJobsDictionary.get(abbHostname, None)
    # print possiblejobs
    jobBeginAt = int(time.mktime(starttime.utctimetuple())) - time.timezone
    jobEndAt = int(time.mktime(endtime.utctimetuple())) - time.timezone
    flag = None
    if possiblejobs:
        for job in possiblejobs:
            #print job
            LoginDisconnectionTimeAndSoOn = jobLoginDisconnectionAndSoOnDictionary[job]
            retrieved_loginTime = LoginDisconnectionTimeAndSoOn[0]
            retrieved_disconnectionTime = LoginDisconnectionTimeAndSoOn[1]
            if (not retrieved_loginTime):
                loginTime = 0
            else:
                loginTime = int(retrieved_loginTime)
            if (not retrieved_disconnectionTime):
                disconnectionTime = int(time.time())+100
            else:
                disconnectionTime = int(retrieved_disconnectionTime)
            if ((loginTime >= jobBeginAt+0) and (loginTime <= jobBeginAt + 600)):
                if ((jobEndAt >= disconnectionTime+0) and (jobEndAt <= disconnectionTime + 600)):
	     	    #print localjobid, commonname, host, starttime, endtime
        	    retrieved_filename = LoginDisconnectionTimeAndSoOn[2]
                    if (not retrieved_filename):
                        retrieved_filename = "None"
                    retrieved_redirectionsite = LoginDisconnectionTimeAndSoOn[3]
                    if (not retrieved_redirectionsite):
                        retrieved_redirectionsite = "None"
                    if redirectionsite_vs_users_dictionary.get(retrieved_redirectionsite, None):
                        redirectionsite_vs_users_dictionary[retrieved_redirectionsite].add(commonname)
                    else:
                        redirectionsite_vs_users_dictionary[retrieved_redirectionsite]=Set([commonname])
                    # we need also update another
                    key_of_redirectionsiteuser = retrieved_redirectionsite + "."+ commonname
		    str_gmstarttime = gmstarttime.strftime("%Y-%m-%d %H:%M:%S GMT")
	   	    str_gmendtime = gmendtime.strftime("%Y-%m-%d %H:%M:%S GMT")
                    if (not redirectionsiteuser_vs_jobs_dictionary.get(key_of_redirectionsiteuser, None)):
                        redirectionsiteuser_vs_jobs_dictionary[key_of_redirectionsiteuser] = Set([localjobid+", "+str_gmstarttime + "--" + str_gmendtime + ", \n          " + retrieved_filename])
                    else:
                        redirectionsiteuser_vs_jobs_dictionary[key_of_redirectionsiteuser].add(localjobid+", "+str_gmstarttime + "--" + str_gmendtime + ", \n          "+retrieved_filename)
		    #for key, value in redirectionsite_vs_users_dictionary.iteritems():
		    #	print key
		    #    print value
		    #for key, value in redirectionsiteuser_vs_jobs_dictionary.iteritems():
		    #	print key
		    #	print value			                        
                    flag = 1
    return flag

'''                    
 the following two hash tables are defined so that we can output the following content easier:
 for cmssrv32.fnal.gov (a redirection site)
   for user /....../CN=Brian (a x509UserProxyVOName)
       1234.0, 8:00-12:00, /store/foo
'''
redirectionsite_vs_users_dictionary = {}
redirectionsiteuser_vs_jobs_dictionary = {}

jobLoginDisconnectionAndSoOnDictionary = {}
hostnameJobsDictionary  = {}

def buildJobLoginDisconnectionAndSoOnDictionary(filename):
    '''
    build a dictonary in such a format by scanning the xrootd logs
    key     value
    jobid   [login time, disconnection time, filename, redirection site]
    '''
    infile = open(filename)
    # we scan this line
    while 1:
        line = infile.readline()
        if not line:
            break
        # judge whether it is a login record 
        matchflagLogin = re.match("(\d{2})(\d{2})(\d{2}) (\d{2}:\d{2}:\d{2}) \d+ XrootdXeq: (\S+) login\s*", line, 0)
        if matchflagLogin:
            # we try to build a dictionary
            TheLoginDatetime = "20"+matchflagLogin.group(1)+"-"+matchflagLogin.group(2)+"-"+matchflagLogin.group(3)+" "+matchflagLogin.group(4); 
            logintimestamp =  int(time.mktime(time.strptime(TheLoginDatetime, '%Y-%m-%d %H:%M:%S')))
            jobid = matchflagLogin.group(5)
            curjobLoginDisconnectionAndSoOn = jobLoginDisconnectionAndSoOnDictionary.get(jobid, None)
            if (not curjobLoginDisconnectionAndSoOn):
                curjobLoginDisconnectionAndSoOn = [None, None, None, None]
                # includes login time, disconnection time, filename, and redirection site
            curjobLoginDisconnectionAndSoOn[0] = logintimestamp
            jobLoginDisconnectionAndSoOnDictionary[jobid] = curjobLoginDisconnectionAndSoOn
            jobiditems = jobid.split("@")
            currenthostname = jobiditems[1]
            # this is a full host name
            # currentjobs = hostnamejobsDictionary[currenthostname]
            currentjobs = hostnameJobsDictionary.get(currenthostname, None)
            if (not currentjobs):
                currentjobs=[]
            #print jobid
            currentjobs.append(jobid)
            hostnameJobsDictionary[currenthostname] = currentjobs
            #print currenthostname
            #print hostnameJobsDictionary[currenthostname]
        else: # else we judge whether it is a disconnection record
            matchflagDisconnection = re.match("(\d{2})(\d{2})(\d{2}) (\d{1,2}:\d{2}:\d{2}) \d+ XrootdXeq: (\S+) disc \d{1,2}:\d{2}:\d{2}\n", line)
            if matchflagDisconnection:
                # we try to 
                TheDisconnectionDatetime = "20"+matchflagDisconnection.group(1)+"-"+matchflagDisconnection.group(2)+"-"+matchflagDisconnection.group(3)+" "+matchflagDisconnection.group(4); 
                disconnectiontimestamp =  int(time.mktime(time.strptime(TheDisconnectionDatetime, '%Y-%m-%d %H:%M:%S')))
                jobid = matchflagDisconnection.group(5)
                curjobLoginDisconnectionAndSoOn = jobLoginDisconnectionAndSoOnDictionary.get(jobid, None)
                if (not curjobLoginDisconnectionAndSoOn):
                    curjobLoginDisconnectionAndSoOn = [None, None, None, None]
                curjobLoginDisconnectionAndSoOn[1] = disconnectiontimestamp
                jobLoginDisconnectionAndSoOnDictionary[jobid] = curjobLoginDisconnectionAndSoOn
            else:
                matchflagFilenameRedirectionsite = re.match("\d{6} \d{1,2}:\d{2}:\d{2} \d+ Decode xrootd redirects (\S+) to (\S+) (\S+)\n", line)
                if matchflagFilenameRedirectionsite:
                    # we try to
                    jobid = matchflagFilenameRedirectionsite.group(1)
                    redirectionsite = matchflagFilenameRedirectionsite.group(2)
                    thisjobfilename = matchflagFilenameRedirectionsite.group(3)
                    curjobLoginDisconnectionAndSoOn = jobLoginDisconnectionAndSoOnDictionary.get(jobid, None)
                    if (not curjobLoginDisconnectionAndSoOn):
                        curjobLoginDisconnectionAndSoOn = [None, None, None, None]
                    curjobLoginDisconnectionAndSoOn[2] = thisjobfilename
                    curjobLoginDisconnectionAndSoOn[3] = redirectionsite
                    jobLoginDisconnectionAndSoOnDictionary[jobid] = curjobLoginDisconnectionAndSoOn
                
    infile.close()
    # now, we show the dictonary
    #for key,value in jobLoginDisconnectionAndSoOnDictionary.iteritems():
     #  print key
      # print value
    #for key, value in hostnameJobsDictionary.iteritems():
    # print key
    # print value

def main():    
    # Get all the filenames in the form of xrootd.log
    # then for each file, build the hash table
    filenames = os.listdir("/var/log/xrootd")
    for filename in filenames:
        if (filename.find("xrootd.log")>=0):
            buildJobLoginDisconnectionAndSoOnDictionary("/var/log/xrootd/"+filename)
    # connect the database server rcf-gratia.unl.edu
    db, cursor = ConnectDatabase()
    # query database gratia, and output statistic results
    QueryGratia(cursor)
    # check with xrootd log, and output possible overflow jobs with exit code 84
    FilterCondorJobs(cursor)
    # disconnect the database
    db.close()
    # output result in the following format
    # for cmssrv32.fnal.gov (a redirection site)
    #   for user /....../CN=Brian (a x509UserProxyVOName)
    #       1234.0, 8:00-12:00, /store/foo
    for key,value in redirectionsite_vs_users_dictionary.iteritems():
        print "for "+ key+":"
        for oneuser in set(value):
            print "    for "+ oneuser+":"
            cur_key_value = key + "."+oneuser
            for onejob in redirectionsiteuser_vs_jobs_dictionary[cur_key_value]:
                print "        "+onejob


# execute main function
if __name__ == "__main__":
    main()

