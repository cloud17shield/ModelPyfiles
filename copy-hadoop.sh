#!/bin/bash
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/core-site.xml gpu17-x1:core-site.xml
echo 'o98k' | ssh gpu17-x1 "sudo -S su -c 'cp /home/hduser/core-site.xml /opt/hadoop-2.7.5/etc/hadoop/core-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/hdfs-site.xml gpu17-x1:hdfs-site.xml
echo 'o98k' | ssh gpu17-x1 "sudo -S su -c 'cp /home/hduser/hdfs-site.xml /opt/hadoop-2.7.5/etc/hadoop/hdfs-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/mapred-site.xml gpu17-x1:mapred-site.xml
echo 'o98k' | ssh gpu17-x1 "sudo -S su -c 'cp /home/hduser/mapred-site.xml /opt/hadoop-2.7.5/etc/hadoop/mapred-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/yarn-site.xml gpu17-x1:yarn-site.xml
echo 'o98k' | ssh gpu17-x1 "sudo -S su -c 'cp /home/hduser/yarn-site.xml /opt/hadoop-2.7.5/etc/hadoop/yarn-site.xml'"

echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/core-site.xml gpu17-x2:core-site.xml
echo 'o98k' | ssh gpu17-x2 "sudo -S su -c 'cp /home/hduser/core-site.xml /opt/hadoop-2.7.5/etc/hadoop/core-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/hdfs-site.xml gpu17-x2:hdfs-site.xml
echo 'o98k' | ssh gpu17-x2 "sudo -S su -c 'cp /home/hduser/hdfs-site.xml /opt/hadoop-2.7.5/etc/hadoop/hdfs-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/mapred-site.xml gpu17-x2:mapred-site.xml
echo 'o98k' | ssh gpu17-x2 "sudo -S su -c 'cp /home/hduser/mapred-site.xml /opt/hadoop-2.7.5/etc/hadoop/mapred-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/yarn-site.xml gpu17-x2:yarn-site.xml
echo 'o98k' | ssh gpu17-x2 "sudo -S su -c 'cp /home/hduser/yarn-site.xml /opt/hadoop-2.7.5/etc/hadoop/yarn-site.xml'"

echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/core-site.xml student49-x1:core-site.xml
echo 'o98k' | ssh student49-x1 "sudo -S su -c 'cp /home/hduser/core-site.xml /opt/hadoop-2.7.5/etc/hadoop/core-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/hdfs-site.xml student49-x1:hdfs-site.xml
echo 'o98k' | ssh student49-x1 "sudo -S su -c 'cp /home/hduser/hdfs-site.xml /opt/hadoop-2.7.5/etc/hadoop/hdfs-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/mapred-site.xml student49-x1:mapred-site.xml
echo 'o98k' | ssh student49-x1 "sudo -S su -c 'cp /home/hduser/mapred-site.xml /opt/hadoop-2.7.5/etc/hadoop/mapred-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/yarn-site.xml student49-x1:yarn-site.xml
echo 'o98k' | ssh student49-x1 "sudo -S su -c 'cp /home/hduser/yarn-site.xml /opt/hadoop-2.7.5/etc/hadoop/yarn-site.xml'"

echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/core-site.xml student49-x2:core-site.xml
echo 'o98k' | ssh student49-x2 "sudo -S su -c 'cp /home/hduser/core-site.xml /opt/hadoop-2.7.5/etc/hadoop/core-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/hdfs-site.xml student49-x2:hdfs-site.xml
echo 'o98k' | ssh student49-x2 "sudo -S su -c 'cp /home/hduser/hdfs-site.xml /opt/hadoop-2.7.5/etc/hadoop/hdfs-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/mapred-site.xml student49-x2:mapred-site.xml
echo 'o98k' | ssh student49-x2 "sudo -S su -c 'cp /home/hduser/mapred-site.xml /opt/hadoop-2.7.5/etc/hadoop/mapred-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/yarn-site.xml student49-x2:yarn-site.xml
echo 'o98k' | ssh student49-x2 "sudo -S su -c 'cp /home/hduser/yarn-site.xml /opt/hadoop-2.7.5/etc/hadoop/yarn-site.xml'"

echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/core-site.xml student50-x1:core-site.xml
echo 'o98k' | ssh student50-x1 "sudo -S su -c 'cp /home/hduser/core-site.xml /opt/hadoop-2.7.5/etc/hadoop/core-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/hdfs-site.xml student50-x1:hdfs-site.xml
echo 'o98k' | ssh student50-x1 "sudo -S su -c 'cp /home/hduser/hdfs-site.xml /opt/hadoop-2.7.5/etc/hadoop/hdfs-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/mapred-site.xml student50-x1:mapred-site.xml
echo 'o98k' | ssh student50-x1 "sudo -S su -c 'cp /home/hduser/mapred-site.xml /opt/hadoop-2.7.5/etc/hadoop/mapred-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/yarn-site.xml student50-x1:yarn-site.xml
echo 'o98k' | ssh student50-x1 "sudo -S su -c 'cp /home/hduser/yarn-site.xml /opt/hadoop-2.7.5/etc/hadoop/yarn-site.xml'"

echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/core-site.xml student50-x2:core-site.xml
echo 'o98k' | ssh student50-x2 "sudo -S su -c 'cp /home/hduser/core-site.xml /opt/hadoop-2.7.5/etc/hadoop/core-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/hdfs-site.xml student50-x2:hdfs-site.xml
echo 'o98k' | ssh student50-x2 "sudo -S su -c 'cp /home/hduser/hdfs-site.xml /opt/hadoop-2.7.5/etc/hadoop/hdfs-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/mapred-site.xml student50-x2:mapred-site.xml
echo 'o98k' | ssh student50-x2 "sudo -S su -c 'cp /home/hduser/mapred-site.xml /opt/hadoop-2.7.5/etc/hadoop/mapred-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/yarn-site.xml student50-x2:yarn-site.xml
echo 'o98k' | ssh student50-x2 "sudo -S su -c 'cp /home/hduser/yarn-site.xml /opt/hadoop-2.7.5/etc/hadoop/yarn-site.xml'"

echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/core-site.xml student51-x1:core-site.xml
echo 'o98k' | ssh student51-x1 "sudo -S su -c 'cp /home/hduser/core-site.xml /opt/hadoop-2.7.5/etc/hadoop/core-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/hdfs-site.xml student51-x1:hdfs-site.xml
echo 'o98k' | ssh student51-x1 "sudo -S su -c 'cp /home/hduser/hdfs-site.xml /opt/hadoop-2.7.5/etc/hadoop/hdfs-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/mapred-site.xml student51-x1:mapred-site.xml
echo 'o98k' | ssh student51-x1 "sudo -S su -c 'cp /home/hduser/mapred-site.xml /opt/hadoop-2.7.5/etc/hadoop/mapred-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/yarn-site.xml student51-x1:yarn-site.xml
echo 'o98k' | ssh student51-x1 "sudo -S su -c 'cp /home/hduser/yarn-site.xml /opt/hadoop-2.7.5/etc/hadoop/yarn-site.xml'"

echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/core-site.xml student51-x2:core-site.xml
echo 'o98k' | ssh student51-x2 "sudo -S su -c 'cp /home/hduser/core-site.xml /opt/hadoop-2.7.5/etc/hadoop/core-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/hdfs-site.xml student51-x2:hdfs-site.xml
echo 'o98k' | ssh student51-x2 "sudo -S su -c 'cp /home/hduser/hdfs-site.xml /opt/hadoop-2.7.5/etc/hadoop/hdfs-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/mapred-site.xml student51-x2:mapred-site.xml
echo 'o98k' | ssh student51-x2 "sudo -S su -c 'cp /home/hduser/mapred-site.xml /opt/hadoop-2.7.5/etc/hadoop/mapred-site.xml'"
echo 'o98k' | scp /opt/hadoop-2.7.5/etc/hadoop/yarn-site.xml student51-x2:yarn-site.xml
echo 'o98k' | ssh student51-x2 "sudo -S su -c 'cp /home/hduser/yarn-site.xml /opt/hadoop-2.7.5/etc/hadoop/yarn-site.xml'"


echo "finished"
