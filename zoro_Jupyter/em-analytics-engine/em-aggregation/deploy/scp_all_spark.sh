if [ $# -ne 2 ]
then
    echo "Usage: $0 <list_of_hosts> <file_to_copy>"
    exit 1
fi

hosts=$1
file=$2

while read host
do
    echo "Running: scp $file to $host"
    scp $file $host:~
done < $hosts
