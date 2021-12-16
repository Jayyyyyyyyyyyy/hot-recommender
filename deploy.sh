#!/usr/bin/env bash
# rsync -avzP target/headpool-recommender-1.0-SNAPSHOT.jar root@tuijian30:/home/td/headpool
mvn clean package -P ucloud -U
for i in {52..53}; do
    scp target/hotvideo-recommender-1.0-SNAPSHOT.jar td@tuijian${i}:/data/hotvideo-recommender/bin
    ssh td@tuijian${i} "cd /data/hotvideo-recommender/bin && ./restart.sh"
done
