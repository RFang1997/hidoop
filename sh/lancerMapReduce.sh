cd src
find . -name *.java |xargs javac

echo On lance le MapReduce
ssh rfang@farman "cd nosave/HIDOOP/src/ ; java application.MyMapReduce line filesample.txt" 

