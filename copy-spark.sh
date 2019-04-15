#!/bin/bash
echo 'o98k' | scp /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf gpu17-x1:spark-defaults.conf
echo 'o98k' | ssh gpu17-x1 "sudo -S su -c 'cp /home/hduser/spark-defaults.conf /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf'"
echo 'o98k' | scp /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf gpu17-x2:spark-defaults.conf
echo 'o98k' | ssh gpu17-x2 "sudo -S su -c 'cp /home/hduser/spark-defaults.conf /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf'"
echo 'o98k' | scp /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf student49-x1:spark-defaults.conf
echo 'o98k' | ssh student49-x1 "sudo -S su -c 'cp /home/hduser/spark-defaults.conf /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf'"
echo 'o98k' | scp /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf student49-x2:spark-defaults.conf
echo 'o98k' | ssh student49-x2 "sudo -S su -c 'cp /home/hduser/spark-defaults.conf /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf'"
echo 'o98k' | scp /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf student50-x1:spark-defaults.conf
echo 'o98k' | ssh student50-x1 "sudo -S su -c 'cp /home/hduser/spark-defaults.conf /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf'"
echo 'o98k' | scp /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf student50-x2:spark-defaults.conf
echo 'o98k' | ssh student50-x2 "sudo -S su -c 'cp /home/hduser/spark-defaults.conf /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf'"
echo 'o98k' | scp /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf student51-x1:spark-defaults.conf
echo 'o98k' | ssh student51-x1 "sudo -S su -c 'cp /home/hduser/spark-defaults.conf /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf'"
echo 'o98k' | scp /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf student51-x2:spark-defaults.conf
echo 'o98k' | ssh student51-x2 "sudo -S su -c 'cp /home/hduser/spark-defaults.conf /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf'"

echo 'o98k' | scp /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh gpu17-x1:spark-env.sh
echo 'o98k' | ssh gpu17-x1 "sudo -S su -c 'cp /home/hduser/spark-env.sh /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh'"
echo 'o98k' | scp /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh gpu17-x2:spark-env.sh
echo 'o98k' | ssh gpu17-x2 "sudo -S su -c 'cp /home/hduser/spark-env.sh /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh'"
echo 'o98k' | scp /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh student49-x1:spark-env.sh
echo 'o98k' | ssh student49-x1 "sudo -S su -c 'cp /home/hduser/spark-env.sh /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh'"
echo 'o98k' | scp /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh student49-x2:spark-env.sh
echo 'o98k' | ssh student49-x2 "sudo -S su -c 'cp /home/hduser/spark-env.sh /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh'"
echo 'o98k' | scp /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh student50-x1:spark-env.sh
echo 'o98k' | ssh student50-x1 "sudo -S su -c 'cp /home/hduser/spark-env.sh /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh'"
echo 'o98k' | scp /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh student50-x2:spark-env.sh
echo 'o98k' | ssh student50-x2 "sudo -S su -c 'cp /home/hduser/spark-env.sh /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh'"
echo 'o98k' | scp /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh student51-x1:spark-env.sh
echo 'o98k' | ssh student51-x1 "sudo -S su -c 'cp /home/hduser/spark-env.sh /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh'"
echo 'o98k' | scp /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh student51-x2:spark-env.sh
echo 'o98k' | ssh student51-x2 "sudo -S su -c 'cp /home/hduser/spark-env.sh /opt/spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh'"


echo "finished"
