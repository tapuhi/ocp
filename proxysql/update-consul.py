import hashlib;
import json
import urllib
from hashlib import sha1;
from itertools import groupby
from operator import itemgetter

import chef
import consul
import requests

orchestrator_address = 'dbmng-mysql-orchestrator0a.42.wixprod.net'
clusters_url = "http://{}:3000/api/clusters-info".format(orchestrator_address)
response = urllib.urlopen(clusters_url)
clusters = json.loads(response.read())
#api = chef.autoconfigure()
api =chef.ChefAPI('https://chef.wixpress.com', '/etc/chef/client.pem','dbmng-mysql-orchestrator0a.42.wixprod.net')
c = consul.Consul(host='sys-hdc2-master0a.42.wixprod.net')


def get_db_user(artifact_id,access_type="rw"):
    db_user=artifact_id.split(".")[-1].split("-")
    db_user=''.join(map(lambda s: s[:2], db_user)) +'_' +access_type
    return db_user


def get_mysql_password(username,production=True):
    m = hashlib.md5()
    m.update(username +('!!' if production else '??'))
    password = m.hexdigest()
    password = '*' + sha1(sha1(password).digest()).hexdigest()
    return password

def get_cluster_id(cluster_name):
    print cluster_name
    bag = chef.DataBag('mysql')
    return bag[cluster_name]['cluster_id']

def get_cluster_users(cluster_name):
    bag = chef.DataBag('mysql')
    return bag[cluster_name]['users']

def cluster_data(cluster_url):
    return requests.get(cluster_url).json()


def set_passwords(cluster_name):
    print cluster_name
    users = get_cluster_users(cluster_name)
    cluster_url = "http://{}:3000/api/cluster/{}".format(orchestrator_address, cluster_name)
    for artifact in users:
        artifact_rw_user=get_db_user(artifact)
        artifact_ro_user=get_db_user(artifact,access_type='ro')
        artifact_rw_passwd=get_mysql_password(artifact_rw_user)
        artifact_ro_passwd=get_mysql_password(artifact_ro_user)
        artifact_ro_data={}
        artifact_ro_data['passwd'] = artifact_ro_passwd
        artifact_rw_data={}
        artifact_rw_data['passwd'] = artifact_rw_passwd
        c.kv.put(str.upper('db/mysql/clusters/{}/users/{}'.format(cluster_name,artifact_ro_user)), json.dumps(artifact_ro_data))
        c.kv.put(str.upper('db/mysql/clusters/{}/users/{}'.format(cluster_name,artifact_rw_user)), json.dumps(artifact_rw_data))





for cluster in clusters:
    try:
        cluster_name = str(cluster['ClusterAlias'])
        cluster_url = "http://{}:3000/api/cluster/{}".format(orchestrator_address, cluster_name)
        cluster_json = cluster_data(cluster_url)
        bag_item_name=str.replace(cluster_name , 'mysql_', '', 1)
        print "ClusterName={}".format(bag_item_name)
        cluster_id=get_cluster_id(bag_item_name)
        print "CLusterID={}".format(cluster_id)
        set_passwords(bag_item_name)

        c.kv.put(str.upper('db/mysql/clusters/{}/cluster_id'.format(bag_item_name)),str(cluster_id))
        cluster_json = sorted(cluster_json,key=itemgetter('DataCenter'))
        masters=[]
        slaves=[]
        for dc,nodes in groupby(cluster_json,key=itemgetter('DataCenter')):
            nodes=sorted(nodes,key=itemgetter('IsCoMaster'))
            #master_slaves=[{'isMaster':isMaster , 'nodes':nds } for isMaster,nds in groupby(nodes,key=itemgetter('IsCoMaster'))]
            for is_master,cluster_nodes in groupby(nodes,key=itemgetter('IsCoMaster')):
                slaves=[node['Key']['Hostname'] for node in cluster_nodes]
                master_slave = 'masters' if is_master else 'slaves'
                for slave in slaves:
                    data={}
                    data['hostname'] = slave
                    data['port']=3306
                    c.kv.put(str.upper('db/mysql/clusters/{}/dcs/{}/{}/{}/'.format(bag_item_name,dc,master_slave,slave)), None)
                    c.kv.put(str.upper('db/mysql/clusters/{}/dcs/{}/{}/{}/data'.format(bag_item_name,dc,master_slave,slave)), json.dumps(data))
    except:
        None

