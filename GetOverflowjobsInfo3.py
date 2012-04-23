
# For each job that is an overflow job by reading "condor_history"
#     we have this job's localjobid, commonname, host, starttime, endtime
#     we check xrootdlog
#        for all jobs in xrootdlog whose hostnames matches the lastremotehost
#            if its starttime >= login and <= login+10min
#              and its endtime >= disconnection and <= disconnection+10min
#            then we print out this job (localjobid, commonname, filename, redirectionsite)



# Okay, got it
# Now, Brian asked me to print this out
# Xrootd Job Failure Report:
# For cmssrv31.fnal.gov (redirectionsite?)
#   For user /..../CN=Brian (commonname)
#     localjobid, starttime-endtime, filename

# Let me figure this out
# build up two hash tables, one is recirectionsite - > users
# the other is redirectionsite.users - > a list of jobs, 
# 					each job is a string, including localjobid, starttime, endtime, filename
              
# then we output according the above two hash tables

#!/usr/bin/python

import os
from sets import Set
import re
import time
from time import gmtime, strftime
from datetime import datetime, date, timedelta;
import MySQLdb

numFoundJobs = 0

gratiaQuery = """
SELECT
   JUR.dbid, LocalJobId, CommonName, Host, StartTime, EndTime
FROM JobUsageRecord JUR
LEFT JOIN Resource RESC ON ((JUR.dbid = RESC.dbid) AND (RESC.description="ExitCode"))
LEFT JOIN JobUsageRecord_Meta JURM ON JUR.dbid = JURM.dbid
WHERE
   EndTime >= %s AND
   EndTime < %s AND
   ResourceType="BatchPilot" AND
   RESC.value=84 AND
   HostDescription LIKE "%%-overflow"
"""

def FilterCondorJobs(start_date):
    
    # open database connection, okay, succeed the first step
    db = MySQLdb.connect("rcf-gratia.unl.edu", "yzheng", "h39GHigNz", "gratia", 49152)

    # what we want 
    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    now = start_date
    yesterday = start_date - timedelta(1)

    # WARNING: this queries for jobs that ended the *previous day* in Pacific time.
    LastestEndTime = now.strftime('%Y-%m-%d 14:00:00')
    EarliestEndTime = yesterday.strftime('%Y-%m-%d 14:00:00')

    print gratiaQuery
    cursor.execute(gratiaQuery, (LastestEndTime, EarliestEndTime))

    # Now we want to handle each record
    numrows = int(cursor.rowcount)

    #print "... the following are overflow jobs within 36 hours ... "
    for i in range(numrows):
        row = cursor.fetchone()
        print row[0], row[1], row[2], row[3], row[4], row[5]
        localjobid = row[1]
        commonname = row[2]
        host = row[3]
        starttime = row[4]
        endtime = row[5]
	gmstarttime = starttime
	gmendtime = endtime
	#print host
	if host != "NULL":
            matchedflag = CheckJobMatchInXrootdLog(localjobid, commonname, host, starttime, endtime, gmstarttime, gmendtime)
    #print "... end of the overflow jobs in 36 hours ... "
    # disconnect from server
    db.close()

    #print strftime("This program ends at ... %Y-%m-%d %H:%M:%S GMT", gmtime())


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
    # mktime assumes local time, but starttime is in UTC.
    # Use "utctimetuple" to force it to a timestamp *forcing no DST*, and
    # subtract off the timezone.  This procedure is DST-safe.
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
    loginRegexp = re.compile("(\d{2})(\d{2})(\d{2}) (\d{2}:\d{2}:\d{2}) \d+ XrootdXeq: (\S+) login\s*")
    disconnectRegexp = re.compile("(\d{2})(\d{2})(\d{2}) (\d{1,2}:\d{2}:\d{2}) \d+ XrootdXeq: (\S+) disc \d{1,2}:\d{2}:\d{2}\n")
    redirectRegexp = re.compile("\d{6} \d{1,2}:\d{2}:\d{2} \d+ Decode xrootd redirects (\S+) to (\S+) (\S+)\n")

    # we scan this line
    while 1:
        line = infile.readline()
        if not line:
            break
        # we scan the xrootdlog file, and we build a hash table
        matchflagLogin = loginRegexp.match(line, 0)
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
            matchflagDisconnection = disconnectRegexp.match(line)
            if matchflagDisconnection:
                # we try to 
                TheDisconnectionDatetime = matchflagDisconnection.group(1)+"-"+matchflagDisconnection.group(2)+"-"+matchflagDisconnection.group(3)+" "+matchflagDisconnection.group(4); 
                disconnectiontimestamp =  int(time.mktime(time.strptime(TheDisconnectionDatetime, '%y-%m-%d %H:%M:%S')))
                jobid = matchflagDisconnection.group(5)
                curjobLoginDisconnectionAndSoOn = jobLoginDisconnectionAndSoOnDictionary.get(jobid, None)
                if (not curjobLoginDisconnectionAndSoOn):
                    curjobLoginDisconnectionAndSoOn = [None, None, None, None]
                curjobLoginDisconnectionAndSoOn[1] = disconnectiontimestamp
                jobLoginDisconnectionAndSoOnDictionary[jobid] = curjobLoginDisconnectionAndSoOn
            else:
                matchflagFilenameRedirectionsite = redirectRegexp.match(line)
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

    FilterCondorJobs(date.today())

    # the following two hash tables are defined so that we can output the following content easier:
    # for cmssrv32.fnal.gov (a redirection site)
    #   for user /....../CN=Brian (a x509UserProxyVOName)
    #       1234.0, 8:00-12:00, /store/foo
    # redirectionsite_vs_users_dictionary = {}
    # redirectionsiteuser_vs_jobs_dictionary = {}

    # Now, we want to print out the result
    for key,value in redirectionsite_vs_users_dictionary.iteritems():
        print "for "+ key+":"
        for oneuser in set(value):
            print "    for "+ oneuser+":"
            cur_key_value = key + "."+oneuser
            for onejob in redirectionsiteuser_vs_jobs_dictionary[cur_key_value]:
                print "        "+onejob

if __name__ == '__main__':
    main()

