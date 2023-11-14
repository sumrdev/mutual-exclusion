# mutual-exclusion

### Starting the project
To discover the other hosts we have made a list of hosts which it will contiously try to connect to, these are:
- localhost:8080
- localhost:8081
- localhost:8082
- localhost:8083

Change the file hosts.txt to the hosts you want to connect to.


To start peers, change the port number to one of the know ports and it will connect to the other peers.
Remember to do it in separate terminals.
```bash
go run client/client.go localhost 8080 hosts.txt
go run client/client.go localhost 8081 hosts.txt
go run client/client.go localhost 8082 hosts.txt
go run client/client.go localhost 8083 hosts.txt
```

The logfile contains the log of the applications