#echo on genere la clé de la machine courante
#ssh-keygen -t rsa

#echo on copie la clé publique de la machine courante et on la met dans les clés authorisées de Zemanek
#ssh-copy-id rfang@Zemanek

cd src
find . -name *.java |xargs javac

echo On lance les Demons
ssh rfang@ader "cd nosave/HIDOOP/src/ ; java ordo.DaemonImpl 8080" &
ssh rfang@bastie "cd nosave/HIDOOP/src/ ; java ordo.DaemonImpl 8081" &
ssh rfang@bleriot "cd nosave/HIDOOP/src/ ; java ordo.DaemonImpl 8082" &
ssh rfang@boeing "cd nosave/HIDOOP/src/ ; java ordo.DaemonImpl 8083" &
ssh rfang@boucher "cd nosave/HIDOOP/src/ ; java ordo.DaemonImpl 8084" &

echo On lance les Nodes
ssh rfang@ader "cd nosave/HIDOOP/src/ ; java hdfs/DataNode" & 
ssh rfang@bastie "cd nosave/HIDOOP/src/ ; java hdfs/DataNode" &
ssh rfang@bleriot "cd nosave/HIDOOP/src/ ; java hdfs/DataNode" &
ssh rfang@boeing "cd nosave/HIDOOP/src/ ; java hdfs/DataNode" &
ssh rfang@boucher "cd nosave/HIDOOP/src/ ; java hdfs/DataNode" &
ssh rfang@farman "cd nosave/HIDOOP/src/ ; java hdfs/NameNode" &







