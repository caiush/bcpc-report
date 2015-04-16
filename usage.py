#!/bin/env python
"""Silly little script that produces reports on usage"""

import MySQLdb as mdb
import yaml
import itertools
import datetime
from keystoneclient.v2_0 import client as keystone_client
from novaclient.v1_1 import client as nova_client
from cinderclient.v1 import client as cinder_client
import csv

start_time = datetime.datetime(2015,01,01)
end_time = datetime.datetime(2015,04,01)


config = yaml.load(open("usage.yml"))
sql_config = config["mysql"]
con = mdb.connect(sql_config["ip"], 
                  sql_config["user"],
                  sql_config["password"])

now = datetime.datetime.utcnow()
if end_time> now:
    print " resetting end to now"
    end_time = now
period_in_hrs = (end_time - start_time).total_seconds() / 60. / 60.

cost_per_hr = { "m1.tiny" : 0.013,
                "m1.small" : 0.026,
                "m1.medium" : 0.052,
                "m1.large"  : 0.142,
                "m1.xlarge" : 0.56 }

volume_cost_per_gbhr = {"HDD" : 0.10/24./30,
                        "SSD" : 0.20/24./30 }

images_cost_per_gbhr = 0.20/24./30.

quota_costs_per_hr = { "cores" : 0.009,
                       "ram" : 0.009, # per gb
                       "volume_gb" : 0.10/24./30.  # per gb
                       } 

keystone_creds = config["keystone"]

cost_per_hr_custom = 0.60

kclient = keystone_client.Client(username= keystone_creds["username"], password=keystone_creds["password"],
                                 auth_url=keystone_creds["auth_url"], cacert=keystone_creds["ca_cert"],
                                 tenant_name=keystone_creds["tenant"])

nclient = nova_client.Client(keystone_creds["username"], keystone_creds["password"],
                             keystone_creds["tenant"], keystone_creds["auth_url"],
                             service_type="compute", cacert=keystone_creds["ca_cert"])
cclient = cinder_client.Client(keystone_creds["username"], keystone_creds["password"],
                               keystone_creds["tenant"], keystone_creds["auth_url"],
                               service_type="volume", cacert=keystone_creds["ca_cert"])


# grab a list of tenants
project_summary = {}
tenants= kclient.tenants.list()

for t in tenants:
    q = nclient.quotas.get(t.id)
    c = cclient.quotas.get(t.id)
#gigabytes=1000, gigabytes_HDD=-1, gigabytes_SSD=-1, snapshots=10, snapshots_HDD=-1, snapshots_SSD=-1, volumes=10, volumes_HDD=-1, volumes_SSD=-1>
    tid = str(t.id)
    project_summary[tid] = {"name" : str(t.name),
                            "id" : tid,
                            "quota" : { "cores" : q.cores ,
                                        "ram" : q.ram / 1024,
                                        "floating_ips" : q.floating_ips,
                                        "volume_gb" : c.gigabytes,                                              
                                        "total_cost" : 0
                                        } }
    for resource in quota_costs_per_hr.keys():
        if resource in project_summary[tid]["quota"]: 
            project_summary[tid]["quota"]["total_cost"] =  project_summary[tid]["quota"]["total_cost"] +  period_in_hrs*quota_costs_per_hr[resource]* project_summary[tid]["quota"][resource]

print "Number of projects :" , len(project_summary)

# get a list of instances in this time period
instances = []    
con.select_db("nova")
nova_c = con.cursor()
nova_c.execute("""select project_id, launched_at, terminated_at, vm_state, instance_types.name, instance_types.memory_mb, instance_types.vcpus, instance_types.root_gb from instances, instance_types where (launched_at< '%(end_time)s' and (terminated_at > '%(start_time)s' or terminated_at is NULL)) and instance_types.id = instance_type_id order by project_id""" % {"start_time" : start_time, "end_time" : end_time})

row = nova_c.fetchone()
while row:
    project_id, launched_at, terminated_at, vm_state, type_name, mem_mb, cpus, root_gb = row
    last_time = min(terminated_at or end_time, end_time)
    begin_time = max(launched_at, start_time)
    instances.append( { "project_id" : project_id,                        
                        "launched_at" : launched_at,
                        "terminated_at": terminated_at,                        
                        "uptime" : (last_time - begin_time).total_seconds() / 60. / 60., 
                        "vm_state" : vm_state, 
                        "flavor" : type_name,
                        "cpus" : cpus,
                        "mem": mem_mb,
                        "root_gb": root_gb })
    
    row = nova_c.fetchone()

lcpu = lambda x: x["cpus"]
lmem = lambda x: x["mem"]
lroot = lambda x: x["root_gb"]
lcpu_hrs = lambda x: x["cpus"]*x["uptime"]
lmem_hrs = lambda x: x["mem"]*x["uptime"]
lroot_hrs = lambda x: x["root_gb"]*x["uptime"]
lhrs = lambda x: x["uptime"]

print " Number of instances" , len(instances)
#
# nova
#
for project_id, project_instances in itertools.groupby(instances, key = lambda x : x["project_id"]):
    ii = list(project_instances)
    
    p = {
         "cpus" : reduce( lambda x, y: x+y, map(lcpu, ii)),
         "cpuhrs" : reduce( lambda x, y: x+y, map(lcpu_hrs, ii)),
         "mem" : reduce( lambda x, y: x+y, map(lmem, ii)),
         "memhrs" : reduce( lambda x, y: x+y, map(lmem_hrs, ii)),         
         "root" : reduce( lambda x, y: x+y, map(lroot, ii)),
         "roothrs" : reduce( lambda x, y: x+y, map(lroot_hrs, ii)),
         "flavors" : {} }
    
    ii.sort(cmp=lambda x, y: cmp(x["flavor"], y["flavor"]) )    
    for flavor, flavor_instances in itertools.groupby(ii, key = lambda x : x["flavor"]):        
        cost = cost_per_hr.get(flavor, cost_per_hr_custom)
        jj = list(flavor_instances)
        p["flavors"][flavor] = {
            "count" : len(jj),
            "hours" : reduce( lambda x, y: x+y, map(lhrs, jj)),
            "cost" : reduce( lambda x, y: x+y, map(lambda x: x["uptime"]*cost, jj))}

    p["total_cost"] = sum( [i["cost"] for i in p["flavors"].values()])
    if project_id not in project_summary:
        print " Adding project (instances): " , project_id
        project_summary[project_id] = { "id" : project_id, "name" : "?"}
    project_summary[project_id]["instances"] = p

#
# CINDER
#
volumes = []

con.select_db("cinder")
cinder_c = con.cursor()
cinder_c.execute("""select project_id, launched_at, terminated_at, size, volume_types.name from volumes, volume_types where volumes.volume_type_id= volume_types.id and (launched_at< '%(end_time)s' and (terminated_at > '%(start_time)s' or terminated_at is NULL)) order by project_id"""  % {"start_time" : start_time, "end_time" : end_time})
row = cinder_c.fetchone()
while row:
    project_id, launched_at, terminated_at, size, volume_type = row
    last_time = min(terminated_at or end_time, end_time)
    begin_time = max(launched_at, start_time)
    volumes.append( { "project_id" : project_id,                        
                        "launched_at" : launched_at,
                        "terminated_at": terminated_at,                        
                        "uptime" : (last_time - begin_time).total_seconds() / 60. / 60., 
                        "size" : size,
                        "vtype" : volume_type })
    row = cinder_c.fetchone()                       

print " Number of volumes ", len(volumes)
# summarize by project


for project_id, project_volumes in itertools.groupby(volumes, key = lambda x : x["project_id"]):
    ii = list(project_volumes)
    p = {"count" :  len(ii),
         "types" : {} }
    ii.sort(cmp=lambda x, y: cmp(x["vtype"], y["vtype"]) )    
    for vtype, vtype_volumes in itertools.groupby(ii, key = lambda x : x["vtype"]):        
        jj = list(vtype_volumes)
        gbhr = reduce( lambda x, y: x+y, map(lambda x: x["size"]*x["uptime"], jj))
        p["types"][vtype] = {
            "count" : len(jj),
            "gb" : reduce( lambda x, y: x+y, map(lambda x: x["size"], jj)),
            "gbhrs" : gbhr,
            "cost" : gbhr*volume_cost_per_gbhr[vtype]}

    p["total_cost"] = sum( [q["cost"] for q in p["types"].values()])
    if project_id not in project_summary:
        print " Adding project (volumes): " , project_id
        project_summary[project_id] = { "id" : project_id, "name" : "?"}
    project_summary[project_id]["volumes"] = p 

#
# grab the image usage, by project
# 
images = []
con.select_db("glance")
glance_c = con.cursor()
# note as glance doesnt actually free up space, it doesnt matter when this was created.
glance_c.execute("""select owner, size, created_at from images order by owner""")
row = glance_c.fetchone()
while row:
    project_id, isize, created_at = row
    last_time = end_time
    begin_time = max(created_at, start_time)

    images.append( { "project_id" : project_id,
                     "size_gb" : isize / 1024./1024./1024.,
                     "uptime" : (last_time - begin_time).total_seconds() / 60. / 60. })    
    row = glance_c.fetchone()

print " number of images" , len(images)

# group by project
for project_id, project_images in itertools.groupby(images, key = lambda x : x["project_id"]):
    ii = list(project_images)
    gbhr = reduce( lambda x, y: x+y, map(lambda x: x["size_gb"]*x["uptime"], ii))
    p = { "count" : len(ii),
          "size_gb" : sum([x["size_gb"] for x in ii]),
          "gbhrs" : gbhr,
          "total_cost" : gbhr*images_cost_per_gbhr }
    if project_id not in project_summary:
        print "adding project (images): " , project_id
        project_summary[project_id] = { "id" : project_id, "name" : "?"}    
    project_summary[project_id]["images"] = p    
    

projects = project_summary.values() 

projects.sort(cmp = lambda x, y: -1*cmp(x.get("quota", {}).get("total_cost", 0), y.get("quota", {}).get("total_cost", 0)))


def kex(d, path, default=""):
    for k in d:
        if k==path[0]:
            if len(path)==1:
                return d[k]
            else:
                return kex(d[k], path[1:], default)
    return default



columns = [
    ["Name" , ("name",)],
    ["Quota Cost/$", ("quota", "total_cost")],
    ["Quota cores" , ("quota", "cores")],
    ["Quota RAM/GB" , ("quota", "ram")],
    ["Quota Disk/GB" , ("quota", "volume_gb")],
    ["Usage cores" , ("instances", "cpus")],
    ["Usage core hrs/h" , ("instances", "cpuhrs")],
    ["Usage RAM/GB" , ("instances", "mem")],
    ["Usage RAM hrs / GBh" , ("instances", "memhrs")],
    ["Usage root/GB" , ("instances", "root")],
    ["Usage root hrs / GBh" , ("instances", "roothrs")],
]

for flavor in cost_per_hr:
    
    columns.append( [flavor, ("instances","flavors",flavor, "count")]  )
    columns.append([flavor + " hrs" , ("instances","flavors",flavor, "hours")]  )
    columns.append([flavor + " cost", ("instances","flavors",flavor, "cost")])
columns.append(["Instance Usage Cost/$", ("instances","total_cost")])

for vtype in ["HDD", "SSD"]:
    columns+= [
        [vtype+" Count" , ("volumes", "types", vtype, "count")],
        [vtype+" GB" , ("volumes", "types", vtype, "gb")],
        [vtype+" GBhrs" , ("volumes", "types", vtype, "gbhrs")],
        [vtype+" Cost" , ("volumes", "types", vtype, "cost")],
        ]
columns.append(["volumes cost", ("volumes", "total_cost")]    )


columns+= [
    ["Images", ("images", "count")],
    ["Images Size/GB", ("images", "size_gb")],
    ["Images cost/$", ("images", "total_cost")],    
]

with open("output.csv", "wb") as f:
    csvw = csv.writer(f)
    csvw.writerow([c[0] for c in columns])
    for p in projects:
        csvw.writerow( [ kex(p, c[1]) for c in columns])
        
#        for c, k in columns:
#            print c, k, kex(p, k)
        
