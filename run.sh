#!/bin/bash

cd "$(dirname "$0")"

source lib/hd_common.sh

configFile="conf/conf.ini"

hd_basePath="/personal/search/share"
today="$(date -d "-1 day" "+%Y-%m-%d")"

GetSql() {
    #1 fieldFile
    if [ $1 ];then
        sql=""
        for line in $(<$1)
        do
            sql="$sql$line,"
        done
        len=${#sql}
        len=$((len-1))
        sql=${sql:0:$len}
        echo $sql
    fi
    echo ""
} 

Sqoop() {
    #sqoop预处理
    connectUrl="jdbc:mysql://$host:$port/$database?tinyInt1isBit=false"
    cols="$(GetSql $fieldFile)"
    echo $cols
    #primaryKey="product_id"

    #javaFile="QueryResult.java"
    #Remove $javaFile
    Remove $tableName.java
    RemoveHD $mirror_path

    #sqoop 启动
    sqoop import --connect $connectUrl \
    --username $userName \
    --password $password \
    --table $tableName \
    --columns $cols \
    --target-dir $mirror_path   \
    --split-by $primaryKey  \
    --fields-terminated-by '\001' \
    --lines-terminated-by '\n' \
    --null-non-string '' \
    --null-string '' \
    --hive-delims-replacement '' \
    --direct \
    --m 8
}

MapReduce() {
    #mapreduce预处理
    mapFile="maper.py"
    hadoopJar="$HADOOP_HOME/contrib/streaming/hadoop-streaming-0.20.2-cdh3u2.jar"

    #删除两个月以前的数据
    twomonthago="$(date -d"-61 day" +%Y-%m-%d)"
    twomonthago_path=$hd_path/$twomonthago
    RemoveHD $twomonthago_path

    RemoveHD $out_path

    cp $fieldFile "fieldFile"

    #mapreduce启动
    hadoop jar $hadoopJar \
    -D mapred.output.compress=true \
    -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
    -D mapred.job.name="sqoop_to_json" \
    -D mapred.reduce.tasks=0 \
    -input $mirror_path \
    -output $out_path \
    -file  $mapFile \
    -file "fieldFile" \
    -mapper "python $mapFile"
}

Run() {
    tables="$(GetConfig "all" "tables" $configFile)"
    for table in $(echo $tables | tr "," "\n")
    do
        echo $table
        
        host="$(GetConfig $table "host" $configFile)"
        database="$(GetConfig $table "database" $configFile)"
        port="$(GetConfig $table "port" $configFile)"
        userName="$(GetConfig $table "userName" $configFile)"
        password="$(GetConfig $table "password" $configFile)"
        tableName="$(GetConfig $table "table" $configFile)"
        fieldFile="$(GetConfig $table "fieldFile" $configFile)"
        primaryKey="$(GetConfig $table "primaryKey" $configFile)"
     
        #如果目录不存在则创建
        hd_path=$hd_basePath/$table
        MkdirHD $hd_path

        mirror_path=$hd_path/mirror
        out_path=$hd_path/$today

        Sqoop
        echo "$table 的sqoop执行完毕"

        MapReduce
        echo "$table 的mapreduce执行完毕"
    done
}

Run
echo "全部完成"
