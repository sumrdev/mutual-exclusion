package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"mutual-exclusion/proto"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)
type ipAndPort struct{
	ip string 
	port int32 
}
var PORT int32;
var IP string;
var maxPort int32 = 8083
var isAlreadyLocked bool = false 


// build hosts from file called hosts.txt


var hosts = []ipAndPort{
	{"localhost", 8080},
	{"localhost", 8081},
	{"localhost", 8082},
	{"localhost", 8083},
}

var connections = map[ipAndPort]proto.MutualExclusionServiceClient{}
var coordinator *ipAndPort

func readHostsFromFile(file *os.File) (error) {
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		split := strings.Split(line, ":")
		if len(split) != 2 {
			return fmt.Errorf("error when reading file")
		}
		port, err := strconv.Atoi(split[1])
		if err != nil {
			return err
		}
		hosts = append(hosts, ipAndPort{split[0], int32(port)})
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}


func main() {
	
	//Check args
	if len(os.Args) != 4 {
		log.Fatalf("Usage: %s IP PORT file_with_hosts", os.Args[0])
	}
	
	// Open supplied file
	file, err := os.Open(os.Args[3])
	if err != nil {
		log.Fatalf("Error when opening file: %s", err)
	}
	file.Close()

	// Read hosts from file
	readHostsFromFile(file)
	
	//Set IP and PORT
	IP = os.Args[1]
	port, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("PORT must be an integer: %s", err)
	}
	PORT = int32(port)

	// Run gRPC server
	go runServer(PORT)


	// Connect to all hosts
	go connectToHostsContinuously()
	
	// Access critical section continuously
	for {
		time.Sleep(time.Duration(rand.Intn(10)+5) * time.Second)
		accessCriticalSection()

	}
}

func connectToHostsContinuously() {
	for {
		notConnectedHosts := []ipAndPort{}

		//subtract the hosts that are already connected
		for _, host := range hosts{
			val, ok := connections[host]
			if !ok || val == nil {
				notConnectedHosts = append(notConnectedHosts, host)
			}
		}
		for _, host := range notConnectedHosts{
			// Run through all hosts and skip if already connected
			if host.port == int32(PORT) && host.ip == IP { 
				continue
			}
			log.Printf("Connecting to host: %s %d", host.ip, host.port)
			connectToHost(host)
		}
		// Check if coordinator is still alive

		time.Sleep(2 * time.Second)
	}
}

func accessCriticalSection() {
	
	accesRequest := &proto.AccessRequest{
		Message: "Request access",
		Id: PORT,
	}
	fin := &proto.AccessRequest{
		Message: "done",
		Id: PORT,
	}
	if coordinator == nil {
		newElection()
	}
	//coordinator disconnected

	

	
	if coordinator.port == PORT && coordinator.ip == IP {
		mu.Lock()
		log.Printf("Accessing critical section, as coordinator")
		criticalSection()
		log.Printf("Exiting critical section, as coordinator")
		mu.Unlock()
		return
	}
	

	// Check if coordinator is alive
	timeout := 5 * time.Second
	timeoutContext, _ := context.WithTimeout(context.Background(), timeout)
	_, err := connections[*coordinator].HeartBeat(timeoutContext, &proto.EmptyMessage{})
	if err != nil {
		log.Printf("Coordinator is down, calling election")
		newElection()
	}
	waitc := make(chan struct{})
	stream, err := connections[*coordinator].RequestAccess(context.Background())
	if err != nil {
		log.Printf("Error when calling RequestAccess: %s", err)
		return
	}
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				// read done.
				log.Printf("SERVER MADE EOF TRUE")
				close(waitc)
				return
			}
			if err != nil {
				log.Printf("Failed to receive a note : %v", err)
				return
			}
			log.Printf("Got message: %s", in.Message)
			if in.Message == "yes" {
				criticalSection()
				stream.Send(fin)
				stream.CloseSend()
				close(waitc)

			}
			if in.Message == "close"{
				stream.CloseSend()

				return
			}
		}
	}()
	stream.Send(accesRequest)
	<-waitc
}

func criticalSection() {
	log.Printf("Accessing critical section...")
	time.Sleep(5 * time.Second)
	log.Printf("Exiting critical section...")
}

func connectToHost(host ipAndPort) {
	ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)

	conn, err := grpc.DialContext(ctx, host.ip+":"+strconv.Itoa(int(host.port)), grpc.WithBlock(),  grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("could not connect: %v", err)
		return
	}
	if conn == nil {
		log.Printf("Connection is nil")
		return
	}
	log.Printf("Connection State: %s", conn.GetState().String())
	ServiceConn := proto.NewMutualExclusionServiceClient(conn)
	if connections[host] != nil {
		log.Printf("Connection already exists")
		return
	}
	connections[host] = ServiceConn
}

func sendElectionMessage(host ipAndPort) (error) {
	ServiceConn := connections[host]
	ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	log.Printf("Sending election message to host: %s:%d\n", host.ip,  host.port)
	_, err := ServiceConn.Election(ctxTimeout, &proto.EmptyMessage{})
	if err != nil {
		if strings.Contains(err.Error(), "connection refused")  {
			log.Printf("Server is down: %s:%s", host.ip, strconv.Itoa(int(host.port)))
		} else {
			log.Printf("Error when calling MutualExclusionMethod: %s", err)
		}
		return err
	}
	if ctxTimeout.Err() == context.DeadlineExceeded {
		return ctxTimeout.Err()
	}

	return nil
}

// Send election to all hosts with higher port number
func newElection() {
	if (PORT == maxPort) {
		// I am the coordinator
		setIAmCoordinator()
		return
	}
	totalConnections := 0
	errorCount := 0
	// Find new coordinator from hosts with higher id
	for _, host := range hosts {
		if host.port < PORT || connections[host] == nil { 
			continue
		}
		totalConnections += 1
		log.Printf("Sending election message to host: %s %d", host.ip, host.port)
		err := sendElectionMessage(host)
		if (err != nil) {
			errorCount += 1;
		}
	}
	log.Printf("Total connections: %d, connections down: %d", totalConnections, errorCount)
	if totalConnections == errorCount {
		setIAmCoordinator()
	}

}



/////////////// SERVER
func runServer(port int32) {

	grpcServer := grpc.NewServer()
	proto.RegisterMutualExclusionServiceServer(grpcServer, &MutualExclusionServiceServer{})
	listener, err := net.Listen("tcp", ":"+ strconv.Itoa(int(port)))
	if err != nil {
		panic(err)
	}

	if err := grpcServer.Serve(listener); err != nil {
		panic(err)
	}
}

type MutualExclusionServiceServer struct {
	proto.UnimplementedMutualExclusionServiceServer
}

func (s *MutualExclusionServiceServer) Election(ctx context.Context, in *proto.EmptyMessage) (*proto.EmptyMessage, error) {
	// If I am the highest port, I am the coordinator
	if PORT == maxPort { 
		// Send message to the host that sent the election message that I am the coordinator
		setIAmCoordinator()
		return &proto.EmptyMessage{}, nil
	}

	newElection()
	
	// If not the coordinator just send back OK
	return &proto.EmptyMessage{}, nil
}

func electionResultToHost(in *proto.ElectionResult) *ipAndPort {
	return &ipAndPort{in.Ip, in.Port}
}

func (s *MutualExclusionServiceServer) SetCoordinator(ctx context.Context, in *proto.ElectionResult) (*proto.LastCoordinator, error) {
	LastCoordinator := coordinator
	coordinator = electionResultToHost(in)
	log.Printf("Coordinator set to: %d", coordinator.port)
	if(LastCoordinator == nil){
		return nil, nil
	}		
	if(LastCoordinator.port == PORT && LastCoordinator.ip == IP){
		go waitForMutexToBeFree()
	}
	return &proto.LastCoordinator{Id: LastCoordinator.port,}, nil
}

func waitForMutexToBeFree(){
	mu.Lock()
	mu.Unlock()
	connections[*coordinator].FreeAccessOnNewCoordinator(context.Background(),&proto.EmptyMessage{})
}


// Lock only used by coordinator to allow a single client to access critical section
var mu sync.Mutex

func (s *MutualExclusionServiceServer) RequestAccess(stream proto.MutualExclusionService_RequestAccessServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
		  
		  return nil
		}
		if err != nil {
		  return err
		}
		if in.Message == "Request access" {
			//lock the crital section
			log.Printf("Got request to access critical section from port: %d", in.Id )
			mu.Lock()
			if err := stream.Send(&proto.AccessRequest{
				Message: "Access granted, now wait",
				Id: PORT,
			}); err != nil {
				return err
			}
		} else if in.Message == "done" {
			log.Printf("Port %d is done accessing the critical section", in.Id )
			mu.Unlock()
			//free the crital section
			if err := stream.Send(&proto.AccessRequest{
				Message: "close",
				Id: PORT,
			}); err != nil {
				return err
			}
		}
	}
}

func (s *MutualExclusionServiceServer) HeartBeat(ctx context.Context, in *proto.EmptyMessage) (*proto.EmptyMessage, error) {
	return &proto.EmptyMessage{}, nil
}
func(s *MutualExclusionServiceServer)FreeAccessOnNewCoordinator(ctx context.Context, in *proto.EmptyMessage) (*proto.EmptyMessage, error){
	mu.Unlock()
	isAlreadyLocked = false
	return &proto.EmptyMessage{}, nil
}

func setIAmCoordinator (){
	log.Printf("I am the coordinator!")
	lockOnNewCoordinator()
	var lc *ipAndPort;
	for _, conn := range connections {
		lastCoordinator, error := conn.SetCoordinator(context.Background(), &proto.ElectionResult{Ip: IP, Port: PORT})
		if(lastCoordinator != nil){
			lc = &ipAndPort{ip: "localhost", port: lastCoordinator.Id}
		}
		if error != nil {
			log.Printf("Error when calling SetCoordinator: %s", error)
		}
	}
	if(lc==nil){
		isAlreadyLocked=false
		mu.Unlock()
		coordinator = &ipAndPort{ip: IP, port: PORT}
		return
	}
	//send heartbeat to last coordinator
	timeout := 10 * time.Second
	timeoutContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := connections[*lc].HeartBeat(timeoutContext, &proto.EmptyMessage{})
	if err != nil {
		//if no response 
		log.Printf("last coordinator is down, now allowing access to Critical section")
		mu.Unlock()
		isAlreadyLocked = false
	}else{
		//we got response
	}
	coordinator = &ipAndPort{ip: IP, port: PORT}
}
func lockOnNewCoordinator(){
	if(!isAlreadyLocked){
		isAlreadyLocked = true
		mu.Lock()
	}
}

