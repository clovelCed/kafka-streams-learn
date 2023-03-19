DATA=$(curl -X GET http://localhost:8081/subjects | jq -r '.[]')

for D in $DATA;
do
    echo $D
    curl -X DELETE http://localhost:8081/subjects/$D
done