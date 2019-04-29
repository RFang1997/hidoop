cd src
find . -name *.java |xargs javac

echo On lance le Count
ssh rfang@farman "cd nosave/HIDOOP/src/ ; java application.Count filesample.txt" 

