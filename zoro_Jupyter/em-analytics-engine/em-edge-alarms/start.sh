
# Start global
dse spark-submit --master local --class "com.ge.current.ie.analytics.SparkMain" em-edge-alarms-1.0.0.jar --spring.profiles.active=daintree-local EMEdgeAlarmsSparkStream true AMQP

# Start local
dse spark-submit --master local --class "com.ge.current.ie.analytics.SparkMain" em-edge-alarms-1.0.0.jar --spring.profiles.active=daintree-local EMEdgeAlarmsSparkStream false AMQP

# Search for Running App
curl -L "http://localhost:7080" | grep -A 100 "Running Applications" | grep -B 20 Edge

# Kill the job
curl --form id=app-20170413080351-6849 --form terminate=true -X POST "http://10.72.9.212:7080/app/kill"