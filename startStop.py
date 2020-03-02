import requests
import json
import time
import boto3
import subprocess
import sys
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
from msrestazure.azure_exceptions import CloudError



file = open('variables.json',)
data = json.load(file)

def get_credentials():
	subscription_id =data["Cloud"][1]["AZURE_SUBSCRIPTION_ID"]     
	credentials = ServicePrincipalCredentials(
		client_id=data["Cloud"][1]["AZURE_CLIENT_ID"],
		secret=data["Cloud"][1]["AZURE_CLIENT_SECRET"],
		tenant=data["Cloud"][1]["AZURE_TENANT_ID"]
	)
	return credentials, subscription_id


def start_azure_vm():
	credentials, subscription_id = get_credentials()
	resource_client = ResourceManagementClient(credentials, subscription_id)
	compute_client = ComputeManagementClient(credentials, subscription_id)
	VMS_LIST = data["Cloud"][1]["LIST_OF_AZURE_VMS"]

	for vm in range(0,len(VMS_LIST)):
		try:
		# Start the VMs
			print('\nStart VM')
			async_vm_start = compute_client.virtual_machines.start(
				data["Cloud"][1]["GROUP_NAME"], VMS_LIST[vm])
			async_vm_start.wait()
		except CloudError:
			print('A VM operation failed:\n{}'.format(traceback.format_exc()))

def start_aws_vm():
	try:
		print("Starting vms...........")
		response = client.start_instances(InstanceIds=data["Cloud"][0]["LIST_OF_AWS_VMS"])
	except Exception as e:
		print("Encounter error while starting vm : ",str(e))

def start_agent(ipaddress, command):
	HOST=ipaddress
	COMMAND=command
	ssh = subprocess.Popen(["ssh", "%s" % HOST, COMMAND],
					   shell=False,
					   stdout=subprocess.PIPE,
					   stderr=subprocess.PIPE)
	result = ssh.stdout.readlines()
	if result == []:
		error = ssh.stderr.readlines()
		print >>sys.stderr, "ERROR: %s" % error
	else:
		print result



def start_cluster_services(ambariDomain, ambariPort, ambariUserId, ambariUserPw, hdfsClusterName):
	AMBARI_DOMAIN=ambariDomain
	AMBARI_PORT=ambariPort
	AMBARI_USER_ID=ambariUserId
	AMBARI_USER_PW=ambariUserPw
	HDFS_CLUSTER_NAME=hdfsClusterName
	restAPI='/api/v1/clusters/'
	services=['HDFS','YARN', 'MAPREDUCE2','HIVE','ZOOKEEPER','AMBARI_METRICS','SMARTSENSE', 'SPARK2']
	for item in range(0,len(services)):
		url = "http://"+AMBARI_DOMAIN+":"+AMBARI_PORT+restAPI+HDFS_CLUSTER_NAME+"/services/"+services[item]
		data = {"RequestInfo": {"context" :"Starting service via REST"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}

		headers = {"Content-Type": "application/json"}
		try:
		# Starting  each services in a list	
			print("Starting..........", services[item])
			response = requests.put(url, data=json.dumps(data), auth=(AMBARI_USER_ID, AMBARI_USER_PW))
		except Exception as e:
			print("error occured wile staring: ", str(e))
		time.sleep(120)

		try:
		# Checking the status of the services started
			print("Checking the status of the service..........")
			status =requests.get(url, auth=(AMBARI_USER_ID, AMBARI_USER_PW))
			json_data=json.loads(status.text)
			print("Status of service: "json_data['ServiceInfo']['state'])
		except Exception as e:
			print("exception occured: ", str(e))

def stop_aws_instance():
	try:
		print("Stoping vms...........")
		response = client.stop_instances(InstanceIds=data["Cloud"][0]["LIST_OF_AWS_VMS"])
	except Exception as e:
		print("Encounter error while starting vm : ",str(e))

def stop_azure_vm():
	credentials, subscription_id = get_credentials()
	resource_client = ResourceManagementClient(credentials, subscription_id)
	compute_client = ComputeManagementClient(credentials, subscription_id)
	VMS_LIST = data["Cloud"][1]["LIST_OF_AZURE_VMS"]
	for vm in range(0,len(VMS_LIST)):
		try:
		# Stop the VMs
			print('\nStop VM')
			async_vm_stop = compute_client.virtual_machines.power_off(
				data["Cloud"][1]["GROUP_NAME"], VMS_LIST[vm])
			async_vm_stop.wait()
		except CloudError:
			print('A VM operation failed:\n{}'.format(traceback.format_exc()))


if __name__ == "__main__":
	if data["Cloud"][0]["AWS"]=="True":
		if data["Cloud"][0]["CLUSTER"].upper()=="HDFS":
			if data["Cloud"][0]["START"]=="True" and data["Cloud"][0]["STOP"]=="False":
				ambariDomain=data["Cloud"][0]["AMBARI_DOMAIN"]
				ambariPort=data["Cloud"][0]["AMBARI_PORT"]
				ambariUserId=data["Cloud"][0]["AMBARI_USER_ID"] 
				ambariUserPw=data["Cloud"][0]["AMBARI_USER_PW"] 
				hdfsClusterName=data["Cloud"][0]["HDFS_CLUSTER_NAME"]
				start_aws_vm()
				time.sleep(300)
				print('All Vms started successfully!')
				start_agent(ambariDomain,"sudo ambari-server stop")
				for ip in range(0,len(data["Cloud"][0]["CLUSTER_IPS"])):
					start_agent(data["Cloud"][0]["CLUSTER_IPS"][ip],"sudo ambari-agent start")
				start_agent(ambariDomain,"sudo ambari-server start")
				time.sleep(120)
				start_cluster_services(ambariDomain, ambariPort, ambariUserId, ambariUserPw, hdfsClusterName)
			elif data["Cloud"][0]["STOP"]=="True" and data["Cloud"][0]["START"]=="False":
				stop_aws_instance()
				time.sleep(120)
				print('All Vms stopped successfully!')
		elif data["Cloud"][0]["CLUSTER"].upper()=="KUBE":
			if data["Cloud"][0]["START"]=="True" and data["Cloud"][0]["STOP"]=="False":
				start_aws_vm()
				time.sleep(300)
				print('All Vms started successfully!')
			elif data["Cloud"][0]["STOP"]=="True" and data["Cloud"][0]["START"]=="False":
				stop_aws_instance()
				time.sleep(120)
				print('All Vms stopped successfully!')


	elif data["Cloud"][1]["Azure"]=="True":
		if data["Cloud"][1]["CLUSTER"].upper()=="HDFS":
			if data["Cloud"][1]["START"]=="True" and data["Cloud"][1]["STOP"]=="False":
				ambariDomain=data["Cloud"][1]["AMBARI_DOMAIN"]
				ambariPort=data["Cloud"][1]["AMBARI_PORT"]
				ambariUserId=data["Cloud"][1]["AMBARI_USER_ID"] 
				ambariUserPw=data["Cloud"][1]["AMBARI_USER_PW"] 
				hdfsClusterName=data["Cloud"][1]["HDFS_CLUSTER_NAME"]
				start_azure_vm()
				time.sleep(300)
				print('All Vms started successfully!')
				start_agent(ambariDomain,"sudo ambari-server stop")
				for ip in range(0,len(data["Cloud"][1]["CLUSTER_IPS"])):
					start_agent(data["Cloud"][1]["CLUSTER_IPS"][ip],"sudo ambari-agent start")
				start_agent(ambariDomain,"sudo ambari-server start")
				time.sleep(120)
				start_cluster_services(ambariDomain, ambariPort, ambariUserId, ambariUserPw, hdfsClusterName)
			elif data["Cloud"][1]["STOP"]=="True" and data["Cloud"][1]["START"]=="False":
				stop_azure_vm()
				time.sleep(120)
				print('All Vms stopped successfully!')
		elif data["Cloud"][1]["CLUSTER"].upper()=="KUBE":
			if data["Cloud"][1]["START"]=="True" and data["Cloud"][1]["STOP"]=="False":
				start_azure_vm()
				time.sleep(300)
				print('All Vms started successfully!')
			elif data["Cloud"][1]["STOP"]=="True" and data["Cloud"][1]["START"]=="False":
				stop_azure_vm()
				time.sleep(120)
				print('All Vms stopped successfully!')



	

