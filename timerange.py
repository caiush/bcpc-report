

import datetime
import MySQLdb as mdb
import yaml
import numpy
import prettytable
import pytz
import sys

start_time = datetime.datetime(2014,01,01)
end_time = datetime.datetime(2015,05,01)
dt = datetime.timedelta(minutes=60)
now = datetime.datetime.utcnow()
local_tz = pytz.timezone('America/New_York') 

def utc_to_local(utc_dt):
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt) 

if end_time> now:
    print " resetting end to now"
    end_time = now

print end_time, utc_to_local(end_time)

config = yaml.load(open("usage.yml"))
sql_config = config["mysql"]
con = mdb.connect(sql_config["ip"], 
                  sql_config["user"],
                  sql_config["password"])


def gen_periods(start_time, stop_time, delta):
    tstart = start_time 
    tend = start_time+ delta
    while tend < stop_time:
        tstart+= delta
        tend+= delta
        yield (tstart, tend)

con.select_db("nova")
nova_c = con.cursor()
times = []
cpus = []
ram = []
root = []
lastday = 0
for window in gen_periods(start_time, end_time, dt):
    nova_c.execute("""select  sum(instance_types.memory_mb), sum(instance_types.vcpus), sum(instance_types.root_gb) from instances, instance_types where (launched_at< '%(end_time)s' and (terminated_at > '%(start_time)s' or terminated_at is NULL)) and instance_types.id = instance_type_id order by project_id""" % {"start_time" : window[0], "end_time" : window[1]})
    tram, tcpu, troot = nova_c.fetchone()
    local_t = utc_to_local(window[0])
    if local_t.day != lastday:
        lastday = local_t.day
        sys.stdout.write("\r%s" %  local_t)
        sys.stdout.flush()
    times.append(local_t)
    cpus.append(int(tcpu or 0))
    root.append(int(troot or 0))
    ram.append(int(tram or 0))

print "generating stats"

times = numpy.array(times)
cpus = numpy.array(cpus)
root = numpy.array(root) 
ram = numpy.array(ram) / 1024.


t = prettytable.PrettyTable(["resource", "max", "min", "99pct", "avg"])
t.add_row( [ "cpu", cpus.max(), cpus.min(), numpy.percentile(cpus, 99) , int(cpus.mean()) ])
t.add_row( [ "root", root.max(), root.min(), numpy.percentile(root, 99) , root.mean() ])
t.add_row( [ "ram", ram.max(), ram.min(), numpy.percentile(ram, 99) , ram.mean() ])

print t

fout =open("pout.npy", "wb")
numpy.save(fout, times)
numpy.save(fout, cpus)
numpy.save(fout, root)
numpy.save(fout, ram)
fout.close()

    


