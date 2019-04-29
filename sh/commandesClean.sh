#echo on genere la clé de la machine courante
#ssh-keygen -t rsa

#echo on copie la clé publique de la machine courante et on la met dans les clés authorisées de Zemanek
#ssh-copy-id rfang@Zemanek

echo On clean les anciens Daemons
./CleanDaemons.sh

echo On clean les anciens DataNodes
./CleanDataNodes.sh

echo On clean lancien NameNode
./CleanNameNode.sh







