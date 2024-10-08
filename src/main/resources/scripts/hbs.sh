#!/bin/bash
if [ $# -lt 1 ]
then
 echo "No Args Input..."
 exit ;
fi
case $1 in
"start")
 echo " =================== 启动 HBase 集群 ==================="
 ssh hadoop108 "/opt/module/hbase-2.4.11/bin/start-hbase.sh"
;;
"stop")
 echo " =================== 关闭 HBase 集群 ==================="
 ssh hadoop108 "/opt/module/hbase-2.4.11/bin/stop-hbase.sh"
;;
*)
 echo "Input Args Error..."
;;
esac
