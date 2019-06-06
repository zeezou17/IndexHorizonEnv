#!/bin/bash


cd /home/zeeshan/OVGU/SEM2/DBSE/TPCH/tpch-dbgen-master
export DSS_CONFIG=/home/zeeshan/OVGU/SEM2/DBSE/TPCH/tpch-dbgen-master
export DSS_QUERY=$DSS_CONFIG/queries
export DSS_PATH=$DSS_CONFIG/query_output
pwd
# Query numbers = 1,3,5,6,10,12,14
query_files=(1 3 5 6 10 12 14)

#Loop through the number of workloads
# Change the number of workloads as much as you want
#for value in {1..5}
#do
#	echo $value
#	> workload$value.sql
#	# Loop through the list
#	for element in "${query_files[@]}"
#	do
#		echo "$element"
#		./qgen $element | tail -n +4 >> workload$value.sql
#	done
#done


#for element in "${query_files[@]}"
#do
#	echo "$element"
#	
#	for value in {1..5}
#	do
#		if [ $value == 1 ]
#		then
#			./qgen $element | tail -n +4 > workload$value.sql
#		else
#			./qgen $element | tail -n +4 >> workload$value.sql
#		fi
#	done
#done


for element in "${query_files[@]}"
do
	#echo "$element"
	./qgen $element | tail -n +4
done



# How to run
# cd /home/zeeshan/OVGU/SEM2/DBSE/TPCH/CreateWorkload
# sudo bash workload_creation.bash >> workload1.sql
# sudo bash workload_creation.bash >> workload2.sql
# sudo bash workload_creation.bash >> workload3.sql
# sudo bash workload_creation.bash >> workload4.sql
# sudo bash workload_creation.bash >> workload5.sql
