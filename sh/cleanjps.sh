jps | grep "DaemonImpl" | cut -d " " -f 1 | xargs kill
echo "Processus kill (PID) :"
jps | grep "DaemonImpl" | cut -d " " -f 1 | xargs echo
