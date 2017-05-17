if [[ $# -ne 3 ]] ; then
    echo "Usage $0 <username> <password> <driver_id_to_stop>"
    exit 1
fi


dsePath=$(which dse)

userName=$1
password=$2
driverId=$3

dseMasterCmd="$dsePath -u $userName -p $password client-tool spark master-address"

sparkMaster=$($dseMasterCmd)

echo
echo "-----------------------------------"

echo "Spark Master command $dseMasterCmd"
echo "Dse path $dsePath"
echo "Spark Master $sparkMaster"

echo "-----------------------------------"
echo

$dsePath -u $userName -p $password spark-class org.apache.spark.deploy.Client kill $sparkMaster $driverId
