# mutual-exclusion

### Starting the project
To discover the other hosts we have made a list of hosts which it will contiously try to connect to, these are:´
- localhost:8080
- localhost:8081
- localhost:8082
- localhost:8083

To start any peer just run the command with IP and port, note that the ports are hardcoded in the code, so you can only use the ports 8080, 8081, 8082 and 8083. You should also only use localhost, since the port is also considered the process ID.
```bash
go run client/client.go localhost 8080
go run client/client.go localhost 8081
go run client/client.go localhost 8082
go run client/client.go localhost 8083
```
