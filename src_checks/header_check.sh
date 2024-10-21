#!bash

for f in Crimes\ au\ Royaume-Uni/*/*-outcomes.csv;
do
head "$f" -n 1
done;

for f in Crimes\ au\ Royaume-Uni/*/*-stop-and-search.csv;
do
head "$f" -n 1
done;

for f in Crimes\ au\ Royaume-Uni/*/*-street.csv;
do
head "$f" -n 1
done;