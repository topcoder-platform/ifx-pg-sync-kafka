curl -X POST \
  http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -H 'Postman-Token: 807a8b78-efc9-45c1-b921-57fc5774b1f5' \
  -H 'cache-control: no-cache' \
  -d '{
    "TIME": "2019-11-06T07:12:43.553779Z",
    "SCHEMANAME": "testdb",
    "TABLENAME": "test7_2",
    "OPERATION": "INSERT",
    "DATA": {
        "abc": "1",
        "bcd": "2",
        "testingbyte": "no"
    },
    "uniquedatatype": "false"
}'