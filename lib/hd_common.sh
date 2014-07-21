#!/bin/bash

source lib/common.sh

ExistHD() {
    # 0 exists. 1 not exists.
    if [ $1 ];then
        hadoop fs -test -e $1
        if [ $? -eq 0 ];then
            return 0
        fi
    fi
    return 1
}

GetMergeHD() {
    #merge and delete crc file
    if [ $1 ] && [ $2 ];then
        hdfs_new_data=$1
        local_data=$2
        Remove $local_data
        ExistHD $hdfs_new_data
        if [ $? -eq 0 ];then
            echo "getmerge $hdfs_new_data to $local_data"
            hadoop fs -getmerge $hdfs_new_data $local_data

            dotFile="$(dirname $local_data)/.$(basename $local_data).crc"
            Remove $dotFile
        fi
    fi
}

RemoveHD() {
    #remove if exist
    if [ $1 ];then
        ExistHD $1
        if [ $? -eq 0 ];then
            echo "remove $1 from hadoop"
            hadoop fs -rmr $1
        fi
    fi
}

MkdirHD() {
    #mkdir if not exist
    if [ $1 ];then
        ExistHD $1
        if [ $?  -ne 0 ];then
            echo "mkdir $1 from hadoop"
            hadoop fs -mkdir $1
        fi
    fi
}
