# Author: Yaling Zheng
# Jan 2012
# Holland Computing Center, University of Nebraska-Lincoln 

import os
from sets import Set
import re
import time
from time import gmtime, strftime
from datetime import datetime, date, timedelta;
import MySQLdb

numFoundJobs = 0

def FilterCondorJobs():
    
    # open database connection
    db = MySQLdb.connect("rcf-gratia.unl.edu", "yzheng", "h39GHigNz", "gratia", 49152)

    # prepare a cursor oject using cursor() method
    cursor = db.cursor()
    
    now = date.today();

    yesterday = date.today() - timedelta(1);

    # Job Latest End Time
    LatestEndTime = now.strftime('%y-%m-%d 14:00:00');
    
    # Job Earliest End time 
    EarliestEndTime = yesterday.strftime('%y-%m-%d 14:00:00');

    # Compute the number of overflow jobs

    querystring = "SELECT COUNT(*) from JobUsageRecord JUR JOIN JobUsageRecord_Meta JURM on JUR.dbid = JURM.dbid where EndTime >=\"20" + EarliestEndTime + "\" and EndTime < \"20" + LatestEndTime + "\" and HostDescription like '%-overflow' and ResourceType=\"BatchPilot\"";

    print querystring
 
    cursor.execute(querystring);
    
    row = cursor.fetchone();
 
    NumOverflowJobs = int(row[0])
    print NumOverflowJobs
    
    # Compute the number of normal jobs
    
    querystring = "SELECT COUNT(*) from JobUsageRecord where EndTime >=\"20" + EarliestEndTime + "\" and EndTime < \"20" + LatestEndTime + "\" and ResourceType=\"BatchPilot\";"; 
    print querystring
    cursor.execute(querystring);
    row = cursor.fetchone();
    
    NumAllJobs = int(row[0])
    
    NumNormalJobs = NumAllJobs - NumOverflowJobs

    print NumNormalJobs 
    # Compute the percentage of overflow jobs over all jobs
    PercentageOverflowJobs = float(NumOverflowJobs*100)/NumAllJobs;

    print str(PercentageOverflowJobs)+"%"
 
    # Compute the summation of Wall Time of overflow jobs
    querystring = "SELECT SUM(WallDuration) from JobUsageRecord where EndTime>=\"20" + EarliestEndTime + "\" and EndTime < \"20" + LatestEndTime + "\" and HostDescription like '%-overflow' and ResourceType=\"BatchPilot\";"
    print querystring
    cursor.execute(querystring);
    row=cursor.fetchone();
    
    SumWallDurationOverflowJobs = float(row[0])
    
    # Compute the summation of Wall Time of all jobs
    querystring = "SELECT SUM(WallDuration) from JobUsageRecord where EndTime>=\"20" + EarliestEndTime + "\" and EndTime < \"20" + LatestEndTime + "\" and ResourceType=\"BatchPilot\";";
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    SumWallDurationAllJobs = float(row[0])  
    PercentageWallDurationOverflowJobs = float(SumWallDurationOverflowJobs*100)/SumWallDurationAllJobs;
    print str(PercentageWallDurationOverflowJobs)+"%"

    # Compute the percentage of overflow jobs with exit code 0
    querystring = "SELECT COUNT(*) from JobUsageRecord JUR JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description = \"ExitCode\")) where EndTime>=\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and RESC.value=0 and HostDescription like '%-overflow' and ResourceType=\"BatchPilot\";";
    print querystring;
    cursor.execute(querystring)
    row = cursor.fetchone()
    NumOverflowJobsExitCode0 = int(row[0])
    PercentageExitCode0Overflow =float(100*NumOverflowJobsExitCode0)/ NumOverflowJobs
    print str(PercentageExitCode0Overflow) + "%"
   
    # Compute the percentage of normal jobs with exit code 0
    querystring = "SELECT COUNT(*) from JobUsageRecord JUR JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description = \"ExitCode\")) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and RESC.value =0 and HostDescription NOT like '%-overflow' and ResourceType=\"BatchPilot\";";
    print querystring;
    cursor.execute(querystring)
    row = cursor.fetchone()
    NumNormalJobsExitCode0 = int(row[0])
    PercentageExitCode0Normal = float(100*NumNormalJobsExitCode0)/NumNormalJobs
    print str(PercentageExitCode0Normal) + "%"
  
    # Compute the walltime percentage of overflow jobs with exit code 0
    querystring = "SELECT SUM(WallDuration) from JobUsageRecord where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and HostDescription like '%-overflow' and ResourceType=\"BatchPilot\";";
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    WallDurationOverflowJobs = float(row[0])
    print str(WallDurationOverflowJobs)
    
    querystring = "SELECT SUM(WallDuration) from JobUsageRecord JUR JOIN Resource RESC on ((JUR.dbid=RESC.dbid) and (RESC.description=\"ExitCode\")) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and RESC.value = 0 and HostDescription like '%-overflow' and ResourceType=\"BatchPilot\";"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    WallDurationOverflowJobsExitCode0 = float(row[0])   
    print str(WallDurationOverflowJobsExitCode0)
    PercentageWallDurationOverflowJobsExitCode0 = float(100*WallDurationOverflowJobsExitCode0)/WallDurationOverflowJobs;
    print str(PercentageWallDurationOverflowJobsExitCode0)+"%"

    # Compute the walltime percentage of overflow jobs with exit code 84
    querystring = "SELECT SUM(WallDuration) from JobUsageRecord JUR JOIN Resource RESC on ((JUR.dbid=RESC.dbid) and (RESC.description=\"ExitCode\")) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and RESC.value = 84 and HostDescription like '%-overflow' and ResourceType=\"BatchPilot\";"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    WallDurationOverflowJobsExitCode84 = float(row[0])
    print str(WallDurationOverflowJobsExitCode84)
    PercentageWallDurationOverflowJobsExitCode84 = float(100*WallDurationOverflowJobsExitCode84)/WallDurationOverflowJobs;     
    print str(PercentageWallDurationOverflowJobsExitCode84)+"%"

    # Compute the percentage of overflow jobs with exit code 84
    querystring = "SELECT COUNT(*) from JobUsageRecord JUR JOIN Resource RESC on ((JUR.dbid=RESC.dbid) and (RESC.description=\"ExitCode\")) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and RESC.value=84 and HostDescription like '%-overflow' and ResourceType=\"BatchPilot\";"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    NumOverflowJobsExitCode84 = int(row[0])
    PercentageNumOverflowJobsExitCode84 = float(100*NumOverflowJobsExitCode84)/NumOverflowJobs;
    print str(PercentageNumOverflowJobsExitCode84)+"%"

    # Compute the percentage of normal jobs with exit code 84
    querystring = "SELECT COUNT(*) from JobUsageRecord JUR JOIN Resource RESC on ((JUR.dbid=RESC.dbid) and (RESC.description=\"ExitCode\")) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and RESC.value =84 and HostDescription NOT like '%-overflow' and ResourceType=\"BatchPilot\";"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    NumNormalJobsExitCode84 = int(row[0])
    PercentageNumNormalJobsExitCode84 = float(100*NumNormalJobsExitCode84)/NumNormalJobs
    print str(PercentageNumNormalJobsExitCode84)+"%"

    # Compute the percentage of Walltime of overflow jobs with exit code 84 (computed already earlier)

    # Compute the efficiency of overflow jobs

    querystring = "SELECT SUM(CpuUserDuration+CpuSystemDuration), SUM(WallDuration) from JobUsageRecord where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and HostDescription like '%-overflow' and ResourceType=\"BatchPilot\";" 
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    UserAndSystemDurationOverflowJobs = float(row[0])
    WallDurationOverflowJobs = float(row[1])
    EfficiencyOverflowJobs = float(100* UserAndSystemDurationOverflowJobs)/WallDurationOverflowJobs;
    print str(EfficiencyOverflowJobs)+"%"

    # Compute the efficiency of normal jobs
    querystring = "SELECT SUM(CpuUserDuration+CpuSystemDuration), SUM(WallDuration) from JobUsageRecord JUR JOIN Resource RESC on ((JUR.dbid=RESC.dbid) and (RESC.description=\"ExitCode\")) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and HostDescription NOT like '%-overflow' and ResourceType=\"BatchPilot\";"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    UserAndSystemDurationNormalJobs = float(row[0])
    WallDurationNormalJobs = float(row[1])
    EfficiencyNormalJobs = float(100*UserAndSystemDurationNormalJobs)/WallDurationNormalJobs
    print str(EfficiencyNormalJobs) + "%"

    # Compute the percentage of overflow jobs whose efficiency greater than 80%
    querystring = "SELECT COUNT(*) from JobUsageRecord where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and HostDescription like '%-overflow' and ResourceType=\"BatchPilot\" and (CpuUserDuration+CpuSystemDuration)/WallDuration > 0.8; ";
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    NumEfficiencyGT80percentOverflowJobs = int(row[0])
    PercentageEfficiencyGT80percentOverflowJobs = float(100*NumEfficiencyGT80percentOverflowJobs)/NumOverflowJobs
    print str(PercentageEfficiencyGT80percentOverflowJobs) + "%"

    # Compute the percentage of normal jobs whose efficiency greater than 80%
    querystring = "SELECT COUNT(*) from JobUsageRecord where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and HostDescription NOT like '%-overflow' and ResourceType=\"BatchPilot\" and (CpuUserDuration+CpuSystemDuration)/WallDuration > 0.8; ";
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    NumEfficiencyGT80percentNormalJobs = int(row[0])
    PercentageEfficiencyGT80percentNormalJobs = float(100*NumEfficiencyGT80percentNormalJobs)/NumNormalJobs
    print str(PercentageEfficiencyGT80percentNormalJobs) + "%"
 
    # Compute the number of overflow jobs that in %UCSD%, Nebraska, Wisconsin, and %Purdue%
    querystring = "SELECT COUNT(*) from JobUsageRecord JUR JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid) JOIN Probe P on (JURM.ProbeName = P.probename) JOIN Site S on (S.siteid = P.siteid) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and HostDescription like '%-overflow' and ResourceType=\"BatchPilot\" and (SiteName like '%Nebraska%' or SiteName like '%UCSD%' or SiteName like '%Purdue%' or SiteName like '%GLOW%');"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    NumOverflowJobs4sites = int(row[0])
    print str(NumOverflowJobs4sites)
    
    # Compute the number of normal jobs that in %UCSD%, Nebraska, Wisconsin, and %Purdue%
    querystring = "SELECT COUNT(*) from JobUsageRecord JUR JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid) JOIN Probe P on (JURM.ProbeName = P.probename) JOIN Site S on (S.siteid = P.siteid) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and HostDescription not like '%-overflow' and ResourceType=\"BatchPilot\" and (SiteName like '%Nebraska%' or SiteName like '%UCSD%' or SiteName like '%Purdue%' or SiteName like '%GLOW%');"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    NumNormalJobs4sites = int(row[0])
    print str(NumNormalJobs4sites)
  
    # Compute the percentage of overflow jobs over all jobs
    PercentageOverflowJobs4sites = float(100*NumOverflowJobs4sites)/(NumOverflowJobs4sites + NumNormalJobs4sites);
    print str(PercentageOverflowJobs4sites) + "%"
   
    # Compute the percentage of overflow jobs walltime 
    # NOTE improvement: write those satisfying time slot jobs into a table, IS THIS WORTHWHILE, I may need to ask Brian 

    querystring = "SELECT SUM(WallDuration) from JobUsageRecord JUR JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid) JOIN Probe P on (JURM.ProbeName = P.probename) JOIN Site S on (S.siteid = P.siteid) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\" and HostDescription like '%-overflow' and ResourceType=\"BatchPilot\" and (SiteName like '%Nebraska%' or SiteName like '%UCSD%' or SiteName like '%Purdue%' or SiteName like '%GLOW%');"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    WallDurationOverflowJobs4sites = float(row[0])
    print str(WallDurationOverflowJobs4sites)
   
    querystring = "SELECT SUM(WallDuration) from JobUsageRecord JUR JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid) JOIN Probe P on (JURM.ProbeName = P.probename) JOIN Site S on (S.siteid = P.siteid) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"  and ResourceType=\"BatchPilot\" and (SiteName like '%Nebraska%' or SiteName like '%UCSD%' or SiteName like '%Purdue%' or SiteName like '%GLOW%');"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    WallDurationAllJobs4sites = float(row[0])
    print str(WallDurationAllJobs4sites)
    PercentageWallDurationOverflowJobs4sites = float(100*WallDurationOverflowJobs4sites)/WallDurationAllJobs4sites
    print str(PercentageWallDurationOverflowJobs4sites)+"%"

    # Compute the percentage of overflow jobs with exit code 0 in the 4 sites
    querystring = "SELECT COUNT(*) from JobUsageRecord JUR JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description=\"ExitCode\")) JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid) JOIN Probe P on (JURM.ProbeName = P.probename) JOIN Site S on (S.siteid = P.siteid) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\" and HostDescription like '%-overflow' and ResourceType=\"BatchPilot\" and RESC.value=0 and (SiteName like '%Nebraska%' or SiteName like '%UCSD%' or SiteName like '%Purdue%' or SiteName like '%GLOW%');"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    NumOverflowJobsExitCode0foursites = int(row[0])
    PercentageOverflowJobsExitCode0foursites = float(100*NumOverflowJobsExitCode0foursites)/NumOverflowJobs4sites
    print str(PercentageOverflowJobsExitCode0foursites)+"%"

    # Compute the percentage of normal jobs with exit code 0 in the 4 sites
    querystring = "SELECT COUNT(*) from JobUsageRecord JUR JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description=\"ExitCode\")) JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid) JOIN Probe P on (JURM.ProbeName = P.probename) JOIN Site S on (S.siteid = P.siteid) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\" and HostDescription not like '%-overflow' and ResourceType=\"BatchPilot\" and RESC.value=0 and (SiteName like '%Nebraska%' or SiteName like '%UCSD%' or SiteName like '%Purdue%' or SiteName like '%GLOW%');"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    NumNormalJobsExitCode0foursites = int(row[0])
    PercentageNormalJobsExitCode0foursites = float(100*NumNormalJobsExitCode0foursites)/NumNormalJobs4sites
    print str(PercentageNormalJobsExitCode0foursites)+"%"

   
    # Compute the percentage of walltime of overflow jobs with exit code 0 over the walltime of all overflow jobs 
    querystring = "SELECT SUM(WallDuration) from JobUsageRecord JUR JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description=\"ExitCode\")) JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid) JOIN Probe P on (JURM.ProbeName = P.probename) JOIN Site S on (S.siteid = P.siteid) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\" and HostDescription like '%-overflow' and ResourceType=\"BatchPilot\" and RESC.value=0 and (SiteName like '%Nebraska%' or SiteName like '%UCSD%' or SiteName like '%Purdue%' or SiteName like '%GLOW%');"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    WallDurationOverflowJobsExitCode0foursites = int(row[0])
    PercentageWallDurationOverflowJobsExitCode0foursites = float(100*WallDurationOverflowJobsExitCode0foursites)/WallDurationOverflowJobs4sites
    print str(PercentageWallDurationOverflowJobsExitCode0foursites)+"%"

    # Compute the percentage of overflow jobs with exit code 84 in the 4 sites
    querystring = "SELECT COUNT(*) from JobUsageRecord JUR JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description=\"ExitCode\")) JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid) JOIN Probe P on (JURM.ProbeName = P.probename) JOIN Site S on (S.siteid = P.siteid) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\" and HostDescription like '%-overflow' and ResourceType=\"BatchPilot\" and RESC.value=84 and (SiteName like '%Nebraska%' or SiteName like '%UCSD%' or SiteName like '%Purdue%' or SiteName like '%GLOW%');"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    NumOverflowJobsExitCode84foursites = int(row[0])
    PercentageOverflowJobsExitCode84foursites = float(100*NumOverflowJobsExitCode84foursites)/NumOverflowJobs4sites
    print str(PercentageOverflowJobsExitCode84foursites)+"%"

    # Compute the percentage of normal jobs with exit code 84 in the 4 sites
    querystring = "SELECT COUNT(*) from JobUsageRecord JUR JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description=\"ExitCode\")) JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid) JOIN Probe P on (JURM.ProbeName = P.probename) JOIN Site S on (S.siteid = P.siteid) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\" and HostDescription not like '%-overflow' and ResourceType=\"BatchPilot\" and RESC.value=84 and (SiteName like '%Nebraska%' or SiteName like '%UCSD%' or SiteName like '%Purdue%' or SiteName like '%GLOW%');"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    NumNormalJobsExitCode84foursites = int(row[0])
    PercentageNormalJobsExitCode84foursites = float(100*NumNormalJobsExitCode84foursites)/NumNormalJobs4sites
    print str(PercentageNormalJobsExitCode84foursites)+"%"

   
    # Compute the percentage of walltime of overflow jobs with exit code 84 over the walltime of all overflow jobs 
    querystring = "SELECT SUM(WallDuration) from JobUsageRecord JUR JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description=\"ExitCode\")) JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid) JOIN Probe P on (JURM.ProbeName = P.probename) JOIN Site S on (S.siteid = P.siteid) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\" and HostDescription like '%-overflow' and ResourceType=\"BatchPilot\" and RESC.value=84 and (SiteName like '%Nebraska%' or SiteName like '%UCSD%' or SiteName like '%Purdue%' or SiteName like '%GLOW%');"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    WallDurationOverflowJobsExitCode84foursites = int(row[0])
    PercentageWallDurationOverflowJobsExitCode84foursites = float(100*WallDurationOverflowJobsExitCode84foursites)/WallDurationOverflowJobs4sites
    print str(PercentageWallDurationOverflowJobsExitCode84foursites)+"%"

   
    # Compute the efficiency of overflow jobs in the 4 sites

    querystring = "SELECT SUM(CpuUserDuration+CpuSystemDuration),SUM(WallDuration) from JobUsageRecord JUR JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid) JOIN Probe P on (JURM.ProbeName = P.probename) JOIN Site S on (S.siteid = P.siteid) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and HostDescription like '%-overflow'  and ResourceType=\"BatchPilot\" and (SiteName like '%Nebraska%' or SiteName like '%UCSD%' or SiteName like '%Purdue%' or SiteName like '%GLOW%');"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    UserAndSystemDurationOverflowJobs4sites = float(row[0])
    WallDurationOverflowJobs4sites = float(row[1])
    EfficiencyOverflowJobs4sites = float(100* UserAndSystemDurationOverflowJobs4sites)/WallDurationOverflowJobs4sites
    print str(EfficiencyOverflowJobs4sites)+"%"

    # Compute the efficiency of normal jobs in the 4 sites
    querystring = "SELECT SUM(CpuUserDuration+CpuSystemDuration), SUM(WallDuration) from JobUsageRecord JUR JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid) JOIN Probe P on (JURM.ProbeName = P.probename) JOIN Site S on (S.siteid = P.siteid) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and HostDescription not like '%-overflow' and ResourceType=\"BatchPilot\" and (SiteName like '%Nebraska%' or SiteName like '%UCSD%' or SiteName like '%Purdue%' or SiteName like '%GLOW%');"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    UserAndSystemDurationNormalJobs4sites = float(row[0])
    WallDurationNormalJobs4sites = float(row[1])
    EfficiencyNormalJobs4sites = float(100*UserAndSystemDurationNormalJobs4sites)/WallDurationNormalJobs4sites
    print str(EfficiencyNormalJobs4sites) + "%"

    # Compute the percentage of overflow jobs of the 4 sites whose efficiency greater than 80%
    querystring = "SELECT COUNT(*) from JobUsageRecord JUR JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid) JOIN Probe P on (JURM.ProbeName = P.probename) JOIN Site S on (S.siteid = P.siteid) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and HostDescription like '%-overflow' and ResourceType=\"BatchPilot\" and (SiteName like '%Nebraska%' or SiteName like '%UCSD%' or SiteName like '%Purdue%' or SiteName like '%GLOW%') and (CpuUserDuration+CpuSystemDuration)/WallDuration>0.8;"
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    NumEfficiencyGT80percentOverflowJobs4sites = int(row[0])
    PercentageEfficiencyGT80percentOverflowJobs4sites = float(100*NumEfficiencyGT80percentOverflowJobs4sites)/NumOverflowJobs4sites
    print str(PercentageEfficiencyGT80percentOverflowJobs4sites) + "%"

    # Compute the percentage of normal jobs of the 4 sites whose efficiency greater than 80%
    querystring = "SELECT COUNT(*) from JobUsageRecord JUR JOIN JobUsageRecord_Meta JURM on (JUR.dbid = JURM.dbid) JOIN Probe P on (JURM.ProbeName = P.probename) JOIN Site S on (S.siteid = P.siteid) where EndTime>\"20" + EarliestEndTime + "\" and EndTime<\"20" + LatestEndTime + "\"" + " and HostDescription NOT like '%-overflow' and ResourceType=\"BatchPilot\" and (SiteName like '%Nebraska%' or SiteName like '%UCSD%' or SiteName like '%Purdue%' or SiteName like '%GLOW%') and (CpuUserDuration+CpuSystemDuration)/WallDuration > 0.8; ";
    print querystring
    cursor.execute(querystring)
    row = cursor.fetchone()
    NumEfficiencyGT80percentNormalJobs4sites = int(row[0])
    PercentageEfficiencyGT80percentNormalJobs4sites = float(100*NumEfficiencyGT80percentNormalJobs4sites)/NumNormalJobs4sites
    print str(PercentageEfficiencyGT80percentNormalJobs4sites) + "%"
    
    print "\n\n\nAll sites\n"

    print "Overflow: "+str(NumOverflowJobs)+" ("+str(PercentageOverflowJobs)+"% Wall "+str(PercentageWallDurationOverflowJobs)+"%) Normal: "+str(NumNormalJobs)

    print "Exit 0: " + str(PercentageExitCode0Overflow)+"% (vs "+str(PercentageExitCode0Normal)+"%) Wall "+str(PercentageWallDurationOverflowJobsExitCode0)+"%"

    print "Exit 84: " + str(PercentageNumOverflowJobsExitCode84)+"% (vs "+str(PercentageNumNormalJobsExitCode84)+"%) Wall "+str(PercentageWallDurationOverflowJobsExitCode84)+"%"

    print "Efficiency: "+str(EfficiencyOverflowJobs)+"% (vs "+str(EfficiencyNormalJobs)+"%)"

    print "Eff  >80%: "+str(PercentageEfficiencyGT80percentOverflowJobs)+"% (vs "+str(PercentageEfficiencyGT80percentNormalJobs)+"%)"

    print "\nOnly UCSD+Nebraska+Wisconsin+Purdue\n"

    print "Overflow: "+str(NumOverflowJobs4sites)+" ("+str(PercentageOverflowJobs4sites)+"% Wall "+str(PercentageWallDurationOverflowJobs4sites)+"%) Normal: "+str(NumNormalJobs4sites)

    print "Exit 0: " + str(PercentageOverflowJobsExitCode0foursites)+"% (vs "+str(PercentageNormalJobsExitCode0foursites)+"%) Wall "+str(PercentageWallDurationOverflowJobsExitCode0foursites)+"%"

    print "Exit 84: " + str(PercentageOverflowJobsExitCode84foursites)+"% (vs "+str(PercentageNormalJobsExitCode84foursites)+"%) Wall "+str(PercentageWallDurationOverflowJobsExitCode84foursites)+"%"

    print "Efficiency: "+str(EfficiencyOverflowJobs4sites)+"% (vs "+str(EfficiencyNormalJobs4sites)+"%)"

    print "Eff  >80%: "+str(PercentageEfficiencyGT80percentOverflowJobs4sites)+"% (vs "+str(PercentageEfficiencyGT80percentNormalJobs4sites)+"%)"
    
    print "\n\n"

    # Find those overflow jobs whose exit code is 84 and resource type is BatchPilot (why?) 
    querystring = "SELECT JUR.dbid, LocalJobId, CommonName, Host, StartTime, EndTime from JobUsageRecord JUR JOIN Resource RESC on ((JUR.dbid = RESC.dbid) and (RESC.description=\"ExitCode\")) JOIN JobUsageRecord_Meta JURM on JUR.dbid = JURM.dbid where EndTime >= \"20"+ EarliestEndTime + "\" and EndTime < \"20"+ LatestEndTime + "\" and ResourceType=\"BatchPilot\" and RESC.value=84 and HostDescription like '%-overflow';";

    print querystring+"\n";

    cursor.execute(querystring);

    # Now we want to handle each record
    numrows = int(cursor.rowcount)

    #print "... the following are overflow jobs in 1 day... "
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
            matchedflag = CheckJobMatchInXrootdLog(localjobid, commonname, host, starttime, endtime, gmstarttime, gmendtime)
    #print "... end of the overflow jobs in 1 day ... "
    # disconnect from server
    db.close()

    #print strftime("This program ends at ... %Y-%m-%d %H:%M:%S GMT", gmtime())


#Below is the format of analysis of possible overflow xrootd jobs with
#exitcode 84 in the following format

#for cmssrv32.fnal.gov (a redirection site)
#    for user .../.../CN=Brian (a x509UserProxyVOName)
#         locajobid starttime endtime jobfilename 

#Brian and I guess the overflow jobs as follows. First, we search the
#gratia database the overflow jobs with exit code 84 via the following
#SQL statement.

#SELECT JUR.dbid, LocalJobId, CommonName, Host, StartTime, EndTime
#from JobUsageRecord JUR LEFT JOIN Resource RESC on ((JUR.dbid =
#RESC.dbid) and (RESC.description="ExitCode")) LEFT JOIN
#JobUsageRecord_Meta JURM on JUR.dbid = JURM.dbid where EndTime >=
#"2012-03-21 14:00:00" and EndTime < "2012-03-22 14:00:00" and
#RESC.value=84 and HostDescription like '%-overflow';

#Then, for each such job J, we refer the xrootd.unl.edu log file, and
#find corresponding xrootd.unl.edu records by guessing: if xrootd log
#show that there is a job whose host machine matches this job's host
#machine, and whose login time is within 10 minutes of job J's start
#time and whose disconnection time is within 10 minutes of job J's
#disconnection time, then this job is a possible xrootd overflow job.

def CheckJobMatchInXrootdLog(localjobid, commonname, host, starttime, endtime, gmstarttime, gmendtime):
    # we want to check our corresponding xrootd log, and see whether we can track the activity of this job
    # we check the dictionary, there are 2 dictionary
    # jobLoginDisconnectionAndSoOnDictionary and hostnameJobsDictionary
    # when we have these 2 jobs, we want to do what?
    # we want to check the dictionary, and see whether there exist jobs that satisfying the the requirement
    # we first relax the requirement
    # we need to parse the hostname
    # print "****************************"
    # print host
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
    # okay, we tried every possible way to fetch the possible jobs
    # now what?
    # print starttime
    # print endtime
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
                    # then we find such a job
		    # print "Found Such a job!"
                    global numFoundJobs
                    numFoundJobs = numFoundJobs + 1
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

                    
# the following two hash tables are defined so that we can output the following content easier:
# for cmssrv32.fnal.gov (a redirection site)
#   for user /....../CN=Brian (a x509UserProxyVOName)
#       1234.0, 8:00-12:00, /store/foo
redirectionsite_vs_users_dictionary = {}
redirectionsiteuser_vs_jobs_dictionary = {}

jobLoginDisconnectionAndSoOnDictionary = {}
hostnameJobsDictionary  = {}

def buildJobLoginDisconnectionAndSoOnDictionary(filename):
    infile = open(filename)
    # we scan this line
    while 1:
        line = infile.readline()
        if not line:
            break
        # we scan the xrootdlog file, and we build a hash table
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
            # now, this is a full host name
            #currentjobs = hostnamejobsDictionary[currenthostname]
            currentjobs = hostnameJobsDictionary.get(currenthostname, None)
            if (not currentjobs):
                currentjobs=[]
            #print jobid
            currentjobs.append(jobid)
            hostnameJobsDictionary[currenthostname] = currentjobs
            #print currenthostname
            #print hostnameJobsDictionary[currenthostname]
        else:
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
    
# Get all the filenames in the form of xrootd.log
# then for each file, build the hash table
filenames = os.listdir("/var/log/xrootd")
for filename in filenames:
    if (filename.find("xrootd.log")>=0):
	buildJobLoginDisconnectionAndSoOnDictionary("/var/log/xrootd/"+filename)
FilterCondorJobs()

# the following two hash tables are defined so that we can output the following content easier:
# for cmssrv32.fnal.gov (a redirection site)
#   for user /....../CN=Brian (a x509UserProxyVOName)
#       1234.0, 8:00-12:00, /store/foo
# redirectionsite_vs_users_dictionary = {}
# redirectionsiteuser_vs_jobs_dictionary = {}

# Now, we want to print out the result

# how do I print out dictionary results

for key,value in redirectionsite_vs_users_dictionary.iteritems():
    print "for "+ key+":"
    for oneuser in set(value):
        print "    for "+ oneuser+":"
        cur_key_value = key + "."+oneuser
        for onejob in redirectionsiteuser_vs_jobs_dictionary[cur_key_value]:
            print "        "+onejob
# Cool, we are done 
