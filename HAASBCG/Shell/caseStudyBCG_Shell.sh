#!/bin/sh
set -xv

export current_dir=`pwd`
echo ${current_dir}
cd ${current_dir}
mkdir -p ${current_dir}/jar_dir

export jar_dir=${current_dir}/jar_dir

rm -f ${jar_dir}/*


if $(hdfs dfs -test -e hdfs://ip:port/user/HAASBCG/inbound/Primary_Person_use.csv) && $(hdfs dfs -test -e hdfs://ip:port/user/HAASBCG/inbound/Units_use.csv) && $(hdfs dfs -test -e hdfs://ip:port/user/HAASBCG/inbound/Damages_use.csv);
then
    echo "All files are available for spark run"
else
    echo "One or more files are missing"
    echo "status=Failed"
    exit 1
fi

hdfs dfs -get hdfs://ip:port/user/HAASBCG/Dependencies/* ${jar_dir}
hdfs dfs -get hdfs://ip:port/user/HAASBCG/Jar/caseStudyBCG-1.0-SNAPSHOT.jar ${jar_dir}

spark2-submit --class bcg.gamma.caseStudyBCG --master yarn --deploy-mode cluster --conf "spark.sql.shuffle.partitions=2000" --conf "spark.executor.memoryOverhead=5244" --conf "spark.memory.fraction=0.8" --conf "spark.sql.files.maxPartitionBytes=2000" --conf "spark.dynamicAllocation.enabled=true" --files ${jar_dir}/file1.xml,${jar_dir}/file2.xml  --jars ${jar_dir}/dependency1.jar,${jar_dir}/dependency2.jar --driver-memory 8g –-driver-cores 1 --executor-memory 16g --executor-cores 2  ${jar_dir}/caseStudyBCG-1.0-SNAPSHOT.jar
ret_code=$?
if [ $ret_code -ne 0 ];
then 
      echo "status=Failed"
      exit $ret_code
else
      echo "status=Succeeded"
fi


rm -f -r ${jar_dir}


    

