if [ $# -ne 1 ]
then
	echo "Usage: $0 <command_to_run_on_all_spark_nodes>"
		exit 1
		fi

		for i in 4 5 6
		do
			echo "Running $1 on 10.128.43.23$i"
				ssh 10.128.43.23$i $1
				done
