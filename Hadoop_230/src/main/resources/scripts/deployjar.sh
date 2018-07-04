#!/bin/bash

sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/oozie/libtools/
sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/flumeng/lib/
sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/hadoop/client/
sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/hadoopmapreduce/
sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/crunch/lib/


scp /home/ubuntu/Downloads/$1 ubuntu@slave1:/home/ubuntu/Downloads
ssh slave1 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/oozie/libtools/
ssh slave1 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/flumeng/lib/
ssh slave1 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/hadoop/client/
ssh slave1 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/hadoopmapreduce/
ssh slave1 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/crunch/lib/


scp /home/ubuntu/Downloads/$1 ubuntu@slave2:/home/ubuntu/Downloads
ssh slave2 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/oozie/libtools/
ssh slave2 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/flumeng/lib/
ssh slave2 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/hadoop/client/
ssh slave2 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/hadoopmapreduce/
ssh slave2 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/crunch/lib/


scp /home/ubuntu/Downloads/$1 ubuntu@slave3:/home/ubuntu/Downloads
ssh slave3 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/oozie/libtools/
ssh slave3 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/flumeng/lib/
ssh slave3 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/hadoop/client/
ssh slave3 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/hadoopmapreduce/
ssh slave3 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/crunch/lib/


scp /home/ubuntu/Downloads/$1 ubuntu@slave4:/home/ubuntu/Downloads
ssh slave4 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/oozie/libtools/
ssh slave4 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/flumeng/lib/
ssh slave4 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/hadoop/client/
ssh slave4 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/hadoopmapreduce/
ssh slave4 sudo cp /home/ubuntu/Downloads/$1 /opt/cloudera/parcels/CDH5.0.01.cdh5.0.0.p0.47/lib/crunch/lib/