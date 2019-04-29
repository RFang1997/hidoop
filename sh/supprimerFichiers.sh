# le fichier à tester (filesample.txt) se trouve dans le /tmp/data de la machine Bore

ssh rfang@ader "rm /tmp/*.txt" &
ssh rfang@bastie  "rm /tmp/*.txt" & 
ssh rfang@bleriot "rm /tmp/*.txt" &
ssh rfang@boeing  "rm /tmp/*.txt" & 
ssh rfang@boucher "rm /tmp/*.txt" &
ssh rfang@farman "rm /tmp/data/count-res" &
ssh rfang@farman "rm /tmp/data/filesample.txt-res" &
ssh rfang@farman "rm /tmp/data/resfinal-filesample.txt" &


