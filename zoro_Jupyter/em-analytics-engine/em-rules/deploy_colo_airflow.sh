./deploy_all_colo.sh target/em-rules*.jar
./command_all_colo_nodes.sh 'sudo mv ~/em-rules*.jar /app/em/rules/'
./command_all_colo_nodes.sh 'ls -lth /app/em/rules/'
