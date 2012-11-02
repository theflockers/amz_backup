#!/usr/bin/env python
#
#  @name amz-backup.py
#  @desc call the raintoolkit to create snapshot backups on amazon ws
#  @author Leandro Mendes<leandro@vanillabean.com.br>
# 

import os, sys
import _mysql 
import shlex
#from multiprocessing import Queue, Process
from Queue import Queue
from threading import Thread
from subprocess import Popen, PIPE

myname     = 'arcah-database'
rain_app   = '/opt/rain/bin'
rain_home  = '/opt/rain/'
retention  = '7d'
worker_threads = 10

# queue instance
q = Queue()

# credentials
aws_access_id     = 'ACCESS_KEY_HERE'
aws_access_secret = 'SECRET_KEY_HERE'

# commands
BACKUP_VOLUME = '%s/backup-volume'

db = _mysql.connect(host='localhost',user='root',db='amz_backup')

def initenv():
	print 'Initializing backup routine..'
	os.putenv('RAIN_HOME', rain_home)
	os.putenv('AWS_ACCESS_ID', aws_access_id)
	os.putenv('AWS_SECRET_KEY', aws_access_secret)

def create_snapshot(vol):
	cmd = "%s -id %s -r %s" % ( BACKUP_VOLUME % (rain_app), vol[3], vol[5])
	print cmd
	return
	p = Popen( shlex.split(cmd), stdout=PIPE, stderr=PIPE)
	os.waitpid(p.pid, 0)
	if len(p.stderr.read().strip()) != 0:
		print 'ERR:', p.stderr.read().strip()
		sys.exit(-1)

	output = p.stdout.read().strip()	
	snapshot = "INSERT INTO snapshot (volume_id, \
						amazon_id, snapshot_time) VALUES (%s, '%s', now()) " %  \
						(vol[0], output)

	try:
		db.query(snapshot)
		print 'Snapshot backup created: %s' % (output)
	except MySQLError, e:
		print e
	# end function

def worker():
	while True:
		item = q.get()
		create_snapshot(item);
		q.task_done()

# initialize the environments variables
initenv()

machines = ['host1','host2']

volumes = []
for machine in machines:
# getting the machines who needs backup
	query_volumes = "SELECT v.id, vm.id as vm_id, vm.name vm_name, vm.amazon_id AS vm_amazon_id, v.amazon_id AS vol_amazon_id, v.device_name FROM attached_volume av JOIN virtual_machine vm ON (vm.id = av.virtual_machine_id ) JOIN volume v ON (v.id = av.volume_id) WHERE vm.name = '%s'" % (myname)
	db.query(query_volumes)
	volumes.append(db.store_result())

# queue
for i in range(worker_threads):
	t = Thread(target=worker)
	t.daemon = True
	t.start()

for volume in volumes.fetch_row():
	params = [volume[0], volume[1], volume[2], volume[4], volume[5], retention]
	q.put(params)

q.join()
