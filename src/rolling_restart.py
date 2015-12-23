import subprocess
from cm_api.api_client import ApiResource
import logging
from optparse import OptionParser

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

parcel_path = "/opt/cloudera/parcels/CDH"

class Component(object):
  def __init__(self, cluster, service, role, config, hostname):
    self.cluster = cluster
    self.service = service
    self.role = role 
    self.children = []
    self.config = config
    self.hostname = hostname

  def start(self):
    self.pre_start()
    if config.dry_run:
      logger.info("Start Role. role.type=%s, hostname=%s" % (self.role.type, self.hostname))
    else:
      logger.info("Starting Role. role.type=%s, hostname=%s" % (self.role.type, self.hostname))
      self.service.start_roles(self.role.name)[0].wait()
      self.role.exit_maintenance_mode().wait()
      logger.info("Started Role. role.type=%s, hostname=%s" % (self.role.type, self.hostname))
    self.post_start()
    if self.children:
      for child in self.children:
        child.start()

  def pre_start(self):
    pass

  def post_start(self):
    pass

  def stop(self):
    if self.children:
      for child in self.children:
        child.stop()
    if self.role.roleState == "STARTED":
      self.pre_stop()
      if config.dry_run:
        logger.info("Stop Role. role.type=%s, hostname=%s" % (self.role.type, self.hostname))
      else:
        logger.info("Stopping Role. role.type=%s, hostname=%s" % (self.role.type, self.hostname))
        self.role.enter_maintenance_mode().wait()
        self.service.stop_roles(self.role.name)[0].wait()
        logger.info("Stopped Role. role.type=%s, hostname=%s" % (self.role.type, self.hostname))
      self.post_stop()

  def pre_stop(self):
    pass

  def post_stop(self):
    pass

  def add_child(self, child):
    self.children.append(child)

class RegionServer(Component):
  def __init__(self, cluster, service, role, config, hostname):
    super(self.__class__, self).__init__(cluster, service, role, config, hostname)

  def pre_stop(self):
    if self.config.conf_dir:
      cmd = "HBASE_CONF_DIR=%s hbase org.jruby.Main %s/lib/hbase/bin/region_mover.rb unload %s" % (self.config.conf_dir, parcel_path, self.hostname)
    else:
      cmd = "hbase org.jruby.Main %s/lib/hbase/bin/region_mover.rb unload %s" % (parcel_path, self.hostname)

    if config.dry_run:
      logger.info("Unload Regions. command=%s" % (cmd))
    else:
      logger.info("Unloading Regions. hostname=%s" % (self.hostname))
      ret = subprocess.check_call(cmd, shell = True)
      logger.info("Unloaded Regions. hostname=%s" % (self.hostname))

  def post_start(self):
    if self.config.conf_dir:
      cmd = "HBASE_CONF_DIR=%s hbase org.jruby.Main %s/lib/hbase/bin/region_mover.rb load %s" % (self.config.conf_dir, parcel_path, self.hostname)
    else:
      cmd = "hbase org.jruby.Main %s/lib/hbase/bin/region_mover.rb load %s" % (parcel_path, self.hostname)

    if config.dry_run:
      logger.info("Load Regions. command=%s" % (cmd))
    else:
      logger.info("Loading Regions. hostname=%s" % (self.hostname))
      ret = subprocess.check_call(cmd, shell = True)
      logger.info("Loaded Regions. hostname=%s" % (self.hostname))

class Master(Component):
  def __init__(self, cluster, service, role, config, hostname):
    super(self.__class__, self).__init__(cluster, service, role, config, hostname)

class DataNode(Component):
  def __init__(self, cluster, service, role, config, hostname):
    super(self.__class__, self).__init__(cluster, service, role, config, hostname)

  def pre_stop(self):
    if (self.config.enable_datanode_decommission):
      if config.dry_run:
        logger.info("Decommission DataNode. hostname=%s" % (self.hostname))
      else:
        logger.info("Decommissioning DataNode. hostname=%s" % (self.hostname))
        self.role.enter_maintenance_mode().wait()
        self.service.decommission(self.role.name).wait()
        logger.info("Decommissioned DataNode. hostname=%s" % (self.hostname))

  def pre_start(self):
    if (self.config.enable_datanode_decommission):
      if config.dry_run:
        logger.info("Recommission DataNode. hostname=%s" % (self.hostname))
      else:
        logger.info("Recommissioning DataNode. hostname=%s" % (self.hostname))
        self.service.recommission(self.role.name).wait
        logger.info("Recommissioned DataNode. hostname=%s" % (self.hostname))
 
class NameNode(Component):
  def __init__(self, cluster, service, role, config, hostname):
    super(self.__class__, self).__init__(cluster, service, role, config, hostname)

class NameNodes(object):
  def __init__(self, service, active, standby, config, host):
    self.service = service
    self.active = active
    self.standby = standby
    self.config = config
    self.host = host

  def restart(self):
    if self.host:
      if self.host == self.standby.hostname:
        self.standby.stop()
        self.standby.start()
      if self.host == self.active.hostname:
        if config.dry_run:
          logger.info("NameNode failover. active=%s, standby=%s" % (self.active.hostname, self.standby.hostname))
        else:
          logger.info("Starting NameNode failover. active=%s, standby=%s" % (self.active.hostname, self.standby.hostname))
          self.service.failover_hdfs(self.active.role.name, self.standby.role.name).wait()
          logger.info("Completed NameNode failover. active=%s, standby=%s" % (self.active.hostname, self.standby.hostname))
        self.active.stop()
        self.active.start()
    else:
      self.standby.stop()
      self.standby.start()
      if config.dry_run:
        logger.info("NameNode failover. active=%s, standby=%s" % (self.active.hostname, self.standby.hostname))
      else:
        logger.info("Starting NameNode failover. active=%s, standby=%s" % (self.active.hostname, self.standby.hostname))
        self.service.failover_hdfs(self.active.role.name, self.standby.role.name).wait()
        logger.info("Completed NameNode failover. active=%s, standby=%s" % (self.active.hostname, self.standby.hostname))
      self.active.stop()
      self.active.start()

class ResorceManager(Component):
  def __init__(self, cluster, service, role, config, hostname):
    super(self.__class__, self).__init__(cluster, service, role, config, hostname)

class NodeManager(Component):
  def __init__(self, cluster, service, role, config, hostname):
    super(self.__class__, self).__init__(cluster, service, role, config, hostname)

class HistoryServer(Component):
  def __init__(self, cluster, service, role, config, hostname):
    super(self.__class__, self).__init__(cluster, service, role, config, hostname)

class Configure(object):
  def __init__(self):
    self.username = ""
    self.password = ""
    self.host = ""
    self.conf_dir = ""
    self.log_file = ""
    self.dry_run = False
    self.enable_datanode_decommission = False
    self.enable_namenode_restart = False
    self.enable_datanode_restart = False
    self.enable_master_restart = False
    self.enable_regionserver_restart = False
    self.enable_hbase_balancer = False
    self.enable_historyserver_restart = False
    self.enable_resourcemanager_restart = False
    self.enable_nodemanager_restart = False

def rolling_restart(api, cluster, config):
  all_hosts = get_all_hosts(api)
  if config.enable_namenode_restart:
    restart_namenodes(cluster, config, all_hosts) 
  if config.enable_master_restart:
    restart_masters(cluster, config, all_hosts)
  if config.enable_resourcemanager_restart:
    restart_resourcemanagers(cluster, config, all_hosts)
  if config.enable_historyserver_restart:
    restart_historyserver(cluster, config, all_hosts)
  if config.enable_datanode_restart or config.enable_regionserver_restart or config.enable_nodemanager_restart:
    restart_slaves(cluster, config)

def restart_namenodes(cluster, config, all_hosts):
  active = ""
  standby = ""
  service = get_service(cluster, "HDFS")
  for role in service.get_all_roles():
    if role.type == "NAMENODE":
      if role.haStatus ==  "ACTIVE":
        active = NameNode(cluster, service, role, config, all_hosts[role.hostRef.hostId])
      if role.haStatus ==  "STANDBY":
        standby = NameNode(cluster, service, role, config, all_hosts[role.hostRef.hostId])
  nameNode = NameNodes(service, active, standby, config, config.host)
  nameNode.restart()

def restart_masters(cluster, config, all_hosts):
  active = ""
  standby = ""
  service = get_service(cluster, "HBASE")
  for role in service.get_all_roles():
    if config.host and config.host != all_hosts[role.hostRef.hostId]:
      continue
    if role.type == "MASTER":
      if role.haStatus ==  "ACTIVE":
        active = Master(cluster, service, role, config, all_hosts[role.hostRef.hostId])
      if role.haStatus ==  "STANDBY":
        standby = Master(cluster, service, role, config, all_hosts[role.hostRef.hostId])
        standby.stop()
        standby.start()
  if active:
    active.stop()
    active.start()

def restart_historyserver(cluster, config, all_hosts):
  service = get_service(cluster, "YARN")
  for role in service.get_all_roles():
    if config.host and config.host != all_hosts[role.hostRef.hostId]:
      continue
    logger.info(role.type)
    if role.type == "JOBHISTORY":
      historyserver = HistoryServer(cluster, service, role, config, all_hosts[role.hostRef.hostId])
      historyserver.stop()
      historyserver.start()

def restart_resourcemanagers(cluster, config, all_hosts):
  active = ""
  standby = ""
  service = get_service(cluster, "YARN")
  for role in service.get_all_roles():
    if config.host and config.host != all_hosts[role.hostRef.hostId]:
      continue
    if role.type == "RESOURCEMANAGER":
      if role.haStatus ==  "ACTIVE":
        active = ResorceManager(cluster, service, role, config, all_hosts[role.hostRef.hostId])
      if role.haStatus ==  "STANDBY":
        standby = ResorceManager(cluster, service, role, config, all_hosts[role.hostRef.hostId])
        standby.stop()
        standby.start()
  if active:
    active.stop()
    active.start()

def restart_slaves(cluster, config):
  all_hosts = get_all_hosts(api)
  slaves = {}
  hdfs_roles = []
  hbase_roles = []
  yarn_roles = []
  if config.enable_datanode_restart:
    hdfs_service = get_service(cluster, "HDFS")
    hdfs_roles = hdfs_service.get_all_roles()
  if config.enable_regionserver_restart:
    hbase_service = get_service(cluster, "HBASE")
    hbase_roles = hbase_service.get_all_roles()
  if config.enable_nodemanager_restart:
    yarn_service = get_service(cluster, "YARN")
    yarn_roles = yarn_service.get_all_roles()

  for role in hdfs_roles:
    if role.type == "DATANODE" and role.commissionState == "COMMISSIONED":
      if config.host and config.host != all_hosts[role.hostRef.hostId]:
        continue
      slaves[role.hostRef.hostId] = DataNode(cluster, hdfs_service, role, config, all_hosts[role.hostRef.hostId])
  for role in hbase_roles:
    if role.type == "REGIONSERVER" and role.commissionState == "COMMISSIONED":
      if config.host and config.host != all_hosts[role.hostRef.hostId]:
        continue
      if role.hostRef.hostId in slaves:
        parent = slaves[role.hostRef.hostId]
        parent.add_child(RegionServer(cluster, hbase_service, role, config, all_hosts[role.hostRef.hostId]))
        slaves[role.hostRef.hostId] = parent
      else:
        slaves[role.hostRef.hostId] = RegionServer(cluster, hbase_service, role, config, all_hosts[role.hostRef.hostId])
  for role in yarn_roles:
    if role.type == "NODEMANAGER" and role.commissionState == "COMMISSIONED":
      if config.host and config.host != all_hosts[role.hostRef.hostId]:
        continue
      if role.hostRef.hostId in slaves:
        parent = slaves[role.hostRef.hostId]
        parent.add_child(NodeManager(cluster, yarn_service, role, config, all_hosts[role.hostRef.hostId]))
        slaves[role.hostRef.hostId] = parent
      else:
        slaves[role.hostRef.hostId] = NodeManager(cluster, yarn_service, role, config, all_hosts[role.hostRef.hostId])
 
  for k,v in slaves.items():
    v.stop()
    v.start()

def get_cluster(api, clustername):
  cluster = api.get_cluster(clustername)
  return cluster

def get_service(cluster, service_type):
  for service in cluster.get_all_services():
    if service.type == service_type:
      return service

def get_all_hosts(api):
  hosts = {}
  for h in api.get_all_hosts():
    hosts[h.hostId] = h.hostname
  return hosts

def enable_balancer(config):
  if config.conf_dir:
    cmd = "echo 'balance_switch true' | HBASE_CONF_DIR=%s hbase shell" % (config.conf_dir)
  else:
    cmd = "echo 'balance_switch true' | hbase shell"

  if config.dry_run:
    logger.info("Enable HBase balancer. command=%s" % (cmd))
  else:
    logger.info("Enabling HBase balancer.")
    ret = subprocess.check_call(cmd, shell = True)
    logger.info("Enabled HBase balancer.")

def disable_balancer(config):
  if config.conf_dir:
    cmd = "echo 'balance_switch false' | HBASE_CONF_DIR=%s hbase shell" % (config.conf_dir)
  else:
    cmd = "echo 'balance_switch false' | hbase shell"

  if config.dry_run:
    logger.info("Disable HBase balancer. command=%s" % (cmd))
  else:
    logger.info("Disabling HBase balancer.")
    ret = subprocess.check_call(cmd, shell = True)
    logger.info("Disabled HBase balancer.")

def get_api(cmhost, username, password):
  return ApiResource(cmhost, username=username, password=password)

def parse_args():
  usage = "usage: %prog [options] ClouderaManagerHostName ClusterName"
  parser = OptionParser(usage=usage)
  parser.add_option("-u", "--user", dest="username", default="admin",
                  help="Cloudera Manager username",)
  parser.add_option("-p", "--password", dest="password", default="admin",
                  help="Cloudera Manager password")
  parser.add_option("--enable_namenode_restart", action="store_true", dest="enable_namenode_restart", default=False,
                  help="Enable NameNode restart")
  parser.add_option("--enable_master_restart", action="store_true", dest="enable_master_restart", default=False,
                  help="Enable Master restart")
  parser.add_option("--enable_datanode_restart", action="store_true", dest="enable_datanode_restart", default=False,
                  help="Enable DataNode restart")
  parser.add_option("--enable_regionserver_restart", action="store_true", dest="enable_regionserver_restart", default=False,
                  help="Enable RegionServer restart")
  parser.add_option("--enable_datanode_decommission", action="store_true", dest="enable_datanode_decommission", default=False,
                  help="Enable DataNode restart")
  parser.add_option("--enable_hbase_balancer", action="store_true", dest="enable_hbase_balancer", default=False,
                  help="Enable HBase Balancer")
  parser.add_option("--enable_historyserver_restart", action="store_true", dest="enable_historyserver_restart", default=False,
                  help="Enable HistoryServer restart")
  parser.add_option("--enable_resourcemanager_restart", action="store_true", dest="enable_resourcemanager_restart", default=False,
                  help="Enable ResourceManager restart")
  parser.add_option("--enable_nodemanager_restart", action="store_true", dest="enable_nodemanager_restart", default=False,
                  help="Enable NodeManager restart")
  parser.add_option("--host",  dest="host", default=False,
                  help="Target host")
  parser.add_option("--conf_dir",  dest="conf_dir", 
                  help="Configuration directory")
  parser.add_option("-l", "--log_file", dest="log_file",
                  help="log file")
  parser.add_option("-d", "--dry_run", action="store_true", dest="dry_run", default=False,
                  help="dry run")

  (options, args) = parser.parse_args()

  config = Configure()
  config.username = options.username
  config.password = options.password
  config.host = options.host
  config.conf_dir = options.conf_dir
  config.log_file = options.log_file
  config.dry_run = options.dry_run
  config.enable_datanode_decommission = options.enable_datanode_decommission
  config.enable_namenode_restart = options.enable_namenode_restart
  config.enable_datanode_restart = options.enable_datanode_restart
  config.enable_master_restart = options.enable_master_restart
  config.enable_historyserver_restart = options.enable_historyserver_restart 
  config.enable_resourcemanager_restart = options.enable_resourcemanager_restart
  config.enable_regionserver_restart = options.enable_regionserver_restart
  config.enable_nodemanager_restart = options.enable_nodemanager_restart
  config.enable_hbase_balancer = options.enable_hbase_balancer
  if options.enable_datanode_restart:
    config.enable_regionserver_restart = True
    config.enable_nodemanager_restart = True

  if len(args) != 2:
    parser.error("incorrect number of arguments.")

  return (config, args)

(config, args) = parse_args()

if config.log_file:
  sh = logging.FileHandler(config.log_file)
else:
  sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s : %(message)s'))
logger.addHandler(sh)

cm_host = args[0]
cluster_name = args[1]

api = get_api(cm_host, config.username, config.password)
cluster = get_cluster(api, cluster_name)

if config.enable_regionserver_restart:
  disable_balancer(config)

rolling_restart(api, cluster, config)

if config.enable_hbase_balancer:
  enable_balancer(config)
