#! /bin/bash
cd /opt/server && jar -uf server.jar config/config.properties && java -Xms512m -Xmx3000m -XX:PermSize=256M -XX:MaxPermSize=512m -Dlog.dir=/opt/server/server-logs -Dspring.profiles.active=cloud  -cp /opt/server/server.jar:/opt/server/lib/*:/opt/server/hadoop_conf com.eurlanda.datashire.server.DatashireServer