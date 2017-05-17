if [[ $# -ne 4 ]] ; then
    echo "Usage $0 <username> <password> <jar_file_path> <json_payload>"
    exit 1
fi

dsePath=$(which dse)
userName=$1
password=$2
jarFilePath=$3
jsonPayload=$4

#dseMasterCmd="$dsePath -u $userName -p $password client-tool spark master-address"

#sparkMaster=$($dseMasterCmd)
sparkMaster=spark://10.72.9.212:7077
sparkMasterIp=$(echo $sparkMaster | grep -oE "\b([0-9]{1,3}\.){3}[0-9]{1,3}\b")
escapedJarFilePath=$(echo $jarFilePath | sed 's/\//\\\//g')

echo
echo "-----------------------------------"

echo "Spark Master command $dseMasterCmd"
echo "Dse path $dsePath"
echo "Spark Master $sparkMaster"
echo "Spark Master ip $sparkMasterIp"
echo "Escaped Jar file path $escapedJarFilePath"

echo "-----------------------------------"
echo

# Create copy of the original json payload
jsonPayloadCopy="$jsonPayload.cp"
cp $jsonPayload $jsonPayloadCopy

sed -i "s/{spark.master.ip}/$sparkMasterIp/g" $jsonPayloadCopy
sed -i "s/{spark.jar.path}/$escapedJarFilePath/g" $jsonPayloadCopy
sed -i "s/{cassandra.username}/$userName/g" $jsonPayloadCopy
sed -i "s/{cassandra.password}/$password/g" $jsonPayloadCopy

#curl -v -X POST "http://localhost:6066/v1/submissions/create" --header "Content-Type:application/json;charset=UTF-8" --data @$jsonPayloadCopy
curl -v -X POST "http://127.0.1.1:6066/v1/submissions/create" --header "Content-Type:application/json;charset=UTF-8" --data @$jsonPayloadCopy
