# mutual-exclusion

### Starting the project
To discover the other hosts we have made a list of hosts which it will contiously try to connect to, these are:
localhost:8080
localhost:8081
localhost:8082
localhost:8083

To start the first peer you need to run the following command:
go run client/client.go localhost 8080

To start the other peers, change the port number to one of the know ports and it will connect to the other peers.
go run client/client.go localhost 8081
go run client/client.go localhost 8082
go run client/client.go localhost 8083
```