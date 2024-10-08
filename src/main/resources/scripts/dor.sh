#!/bin/bash
case $1 in
    "start")
        for host in hadoop108 ; do
            echo "========== 在 $host 上启动 fe  ========="
            ssh $host "source /etc/profile; /opt/module/doris/fe/bin/start_fe.sh --daemon"
        done
        for host in hadoop108 hadoop109 hadoop110 ; do
            echo "========== 在 $host 上启动 be  ========="
            ssh $host "source /etc/profile; /opt/module/doris/be/bin/start_be.sh --daemon"
        done

       ;;
    "stop")
            for host in hadoop108 ; do
                echo "========== 在 $host 上停止 fe  ========="
                ssh $host "source /etc/profile; /opt/module/doris/fe/bin/stop_fe.sh "
            done
            for host in hadoop108 hadoop109 hadoop110 ; do
                echo "========== 在 $host 上停止 be  ========="
                ssh $host "source /etc/profile; /opt/module/doris/be/bin/stop_be.sh "
            done

           ;;

    *)
        echo "启动出错"
        echo "  start   启动doris集群"
        echo "  stop    停止stop集群"

    ;;
esac
