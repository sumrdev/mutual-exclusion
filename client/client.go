package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"mutual-exclusion/proto"

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
var hosts = []ipAndPort{}

var connections = map[ipAndPort]proto.MutualExclusionServiceClient{}
var coordinator *ipAndPort
var logger *log.Logger
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

	if len(os.Args) != 4 {
		log.Fatalf("Usage: %s IP PORT FILE", os.Args[0])
	}
	
	IP = os.Args[1]
	port, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("PORT must be an integer: %s", err)
	}
	PORT = int32(port)

	f, err := os.OpenFile("logfile", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}

	// Make a log file
	logger = log.New(os.Stdout, "Port: "+strconv.Itoa(int(PORT)) + " ", log.LstdFlags)
	logger.SetOutput(f)
	
	defer f.Close()

	// Open supplied file
	file, err := os.Open(os.Args[3])
	if err != nil {
		logger.Fatalf("Error when opening file: %s", err)
	}
	
	// Read hosts from file
	readHostsFromFile(file)
	file.Close()
	
	logger.Printf("Hosts: %v", hosts)

	go runServer(PORT)

	// Connect to all hosts
	go connectToHostsContinuously()
	
	for {
		//Sleep for random time			
		time.Sleep(time.Duration(rand.Intn(10)+5) * time.Second)
		accessCriticalSection()

	}
}

func connectToHostsContinuously() {
	for {
		notConnectedHosts := []ipAndPort{}

		// Subtract the hosts that are already connected
		for _, host := range hosts{
			val, ok := connections[host]
			if !ok || val == nil {
				notConnectedHosts = append(notConnectedHosts, host)
			}
		}
		for _, host := range notConnectedHosts{
			// Run through all hosts except myself
			if host.port == int32(PORT) && host.ip == IP { 
				continue
			}
			logger.Printf("Connecting to host: %s %d", host.ip, host.port)
			connectToHost(host)
		}
		time.Sleep(2 * time.Second)
	}
}

func accessAsCoordinator() {
	logger.Printf("Trying to access critical section as coordinator")
	mu.Lock()
	logger.Printf("Accessing critical section, as coordinator")
	criticalSection()
	logger.Printf("Exiting critical section, as coordinator")
	mu.Unlock()
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
	if coordinator.port == PORT && coordinator.ip == IP {
		accessAsCoordinator()
		return
	}
	
	// Check if coordinator is alive
	timeout := 5 * time.Second
	timeoutContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := connections[*coordinator].HeartBeat(timeoutContext, &proto.EmptyMessage{})
	if err != nil {
		logger.Printf("Coordinator is down, calling election")
		newElection()
	}
	// If i have become the coordinator
	if coordinator.port == PORT && coordinator.ip == IP {
		accessAsCoordinator()
		return
	}

	waitc := make(chan struct{})
	stream, err := connections[*coordinator].RequestAccess(context.Background())
	if err != nil {
		logger.Printf("Error when calling RequestAccess: %s", err)
		return
	}
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				// read done.
				close(waitc)
				return
			}
			if err != nil {
				logger.Printf("Failed to receive a mesage : %v", err)
				if _, ok := <-waitc; ok {
					close(waitc)
				}
				return
			}
			logger.Printf("Got message: %s", in.Message)
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
	logger.Printf("Accessing critical section...")
	time.Sleep(5 * time.Second)
	logger.Printf("Exiting critical section...")
}

func connectToHost(host ipAndPort) {
	ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)

	conn, err := grpc.DialContext(ctx, host.ip+":"+strconv.Itoa(int(host.port)), grpc.WithBlock(),  grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Printf("could not connect: %v", err)
		return
	}
	if conn == nil {
		logger.Printf("Connection is nil")
		return
	}
	logger.Printf("Connection State: %s", conn.GetState().String())
	ServiceConn := proto.NewMutualExclusionServiceClient(conn)
	if connections[host] != nil {
		logger.Printf("Connection already exists")
		return
	}
	connections[host] = ServiceConn
}

func sendElectionMessage(host ipAndPort) (error) {
	ServiceConn := connections[host]
	ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	logger.Printf("Sending election message to host: %s:%d\n", host.ip,  host.port)
	_, err := ServiceConn.Election(ctxTimeout, &proto.EmptyMessage{})
	if err != nil {
		if strings.Contains(err.Error(), "connection refused")  {
			logger.Printf("Server is down: %s:%s", host.ip, strconv.Itoa(int(host.port)))
		} else {
			logger.Printf("Error when calling MutualExclusionMethod: %s", err)
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
		err := sendElectionMessage(host)
		if (err != nil) {
			errorCount += 1;
		}
	}
	logger.Printf("Total connections above: %d, connections above down: %d", totalConnections, errorCount)
	if totalConnections == errorCount {
		setIAmCoordinator()
	}

}

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

func (s *MutualExclusionServiceServer) SetCoordinator(ctx context.Context, in *proto.ElectionResult) (*proto.EmptyMessage, error) {
	coordinator = electionResultToHost(in)
	logger.Printf("Coordinator set to: %d", coordinator.port)
	return &proto.EmptyMessage{}, nil
}

// Lock used by coordinator to allow a single client to access critical section
var mu sync.Mutex

func (s *MutualExclusionServiceServer) RequestAccess(stream proto.MutualExclusionService_RequestAccessServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
		  	return nil
		}
		if err != nil {
			mu.Unlock()
		  	return err
		}
		if in.Message == "Request access" {
			//lock the crital section
			logger.Printf("Got request to access critical section from port: %d", in.Id )
			mu.Lock()
			if err := stream.Send(&proto.AccessRequest{
				Message: "yes",
				Id: PORT,
			}); err != nil {
				return err
			}
		} else if in.Message == "done" {
			logger.Printf("Port %d is done accessing the critical section", in.Id )
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

func setIAmCoordinator (){
	logger.Printf("I am the coordinator!")
	coordinator = &ipAndPort{ip: IP, port: PORT}
	for _, conn := range connections {
		conn.SetCoordinator(context.Background(), &proto.ElectionResult{Ip: IP, Port: PORT})
	} 
}
