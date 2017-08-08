import json
import urllib
from itertools import groupby
from operator import itemgetter

import chef
import consul
import requests

orchestrator_address = 'dbmng-mysql-orchestrator0a.42.wixprod.net'
clusters_url = "http://{}:3000/api/clusters".format(orchestrator_address)
response = urllib.urlopen(clusters_url)
clusters = json.loads(response.read())
api = chef.autoconfigure()

def get_cluster_id(cluster_name):

    bag = chef.DataBag('dbs')
    return bag[cluster_name]['cluster_id']

def cluster_data(cluster_url):
    return requests.get(cluster_url).json()


c = consul.Consul(host='sys-hdc2-master0a.42.wixprod.net')
for cluster in clusters:
    try:
        cluster_url = "http://{}:3000/api/cluster/{}".format(orchestrator_address, cluster)
        cluster_json = cluster_data(cluster_url)
        bag_item_name=str.replace(str(cluster_json[0]['SuggestedClusterAlias']), 'mysql_', '')
        print "ClusterName={}".format(bag_item_name)
        cluster_id=get_cluster_id(bag_item_name)
        print "CLusterID={}".format(cluster_id)

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

