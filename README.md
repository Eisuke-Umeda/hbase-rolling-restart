Rolling Restart Script
======================
Cloudera Manager APIを用いて、HDFS、HBase、YARNクラスタをローリング再起動するためのスクリプトです。

## Requirement

python2.4

Cloudera Manager API Client
https://github.com/cloudera/cm_api.git

## Installation

1. python-setuptoolsのインストール  

  ```
  $ sudo yum install python-setuptools.noarch
  ```

2. cm_apiのインストール  

  ```
  $ git clone https://github.com/cloudera/cm_api.git
  $ cd cm_api/python
  $ sudo python setup.py install

  ```

3. rolling_restart.pyをcloneして配置


## Usage

```
python rolling_restart.py [options] ClouderaManagerHostName ClusterName
```

### Options

| ショートオプション | ロングオプション | 説明 |
| --- | --- | --- |
| -h | --help | ヘルプの表示 |
| -u | --user=USERNAME | Cloudera Managerのユーザ名 |
| -p | --password=PASSWORD | Cloudera Managerのパスワード |
|  | --enable_namenode_restart | NameNodeを再起動する。 |
|  | --enable_master_restart | Masterを再起動する。 |
|  | --enable_datanode_restart | DataNodeを再起動する。RegionServer、NodeManagerも再起動される。 |
|  | --enable_regionserver_restart | RegionServerを再起動する。 |
|  | --enable_datanode_decommission | DataNodeをデコミッションする。 |
|  | --enable_hbase_balancer | 再起動後にHBaseのバランサーを有効にする。 |
|  | --enable_historyserver_restart | HistoryServerを再起動する。 |
|  | --enable_resourcemanager_restart | ResourceManagerを再起動する。 |
|  | --enable_nodemanager_restart | NodeManagerを再起動する。 |
|  | --host=HOST | プロセスを再起動するサーバを指定する。省略した場合は全てのサーバのプロセスが再起動される。 |
|  | --conf_dir=CONF_DIR | hbase-site.xmlを格納したディレクトリを指定する。 |
