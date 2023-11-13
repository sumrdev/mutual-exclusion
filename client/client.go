package main

import (
	"context"
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
var wg sync.WaitGroup
var hosts = []ipAndPort{
	{"localhost", 8080},
	{"localhost", 8081},
	{"localhost", 8082},
	{"localhost", 8083},
}

var connections = map[ipAndPort]proto.MutualExclusionServiceClient{}
var coordinator *proto.ElectionResult

func main() {

	if len(os.Args) != 3 {
		log.Fatalf("Usage: %s IP PORT", os.Args[0])
	}
	
	IP := os.Args[1]
	port, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("PORT must be an integer: %s", err)
	}
	PORT = int32(port)
	go runServer(PORT)


	// Connect to all hosts
	wg.Add(1)
	go func() {
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
	}() 
	for {
		//sleep for random time		
		
		time.Sleep(time.Duration(rand.Intn(10)+5) * time.Second)
		accessCriticalSection()

	}
}

func accessCriticalSection() {
	
	accesRequest := &proto.AccessRequest{
		Message: "Request access" ,
	}
	fin := &proto.AccessRequest{
		Message: "Finished",
	}
	
	// Check if coordinator is alive
	if coordinator != nil {
		for _, host := range hosts {
			if host.port == coordinator.Port && host.ip == coordinator.Ip {
				waitc := make(chan struct{})
				stream, err := connections[host].RequestAccess(context.Background())
				if err != nil {
					log.Fatalf("Error when calling RequestAccess: %s", err)
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
						log.Fatalf("Failed to receive a note : %v", err)
					  }
					  criticalSection()
					  stream.Send(fin)
					  log.Printf("Got message %s", in.Message)
					}
				  }()
				  stream.Send(accesRequest)
				  stream.CloseSend()
					<-waitc
				break
			}
		}
	} else {
		
		log.Printf("Starting new election from host: %d", PORT)
		foundNewCoordinator := newElection()
		if (!foundNewCoordinator) {
			setIAmCoordinator()
		}
		log.Printf("Coordinator is: %s", coordinator)
		accessCriticalSection()
	}


}
func criticalSection() {
	log.Printf("Doing important work!")
	time.Sleep(5 * time.Second)
	log.Printf("Done with work!")
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
func newElection() bool {
	if (PORT == maxPort) {
		// I am the coordinator
		setIAmCoordinator()
		return true
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
	return totalConnections != errorCount
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

	foundCoordinator := newElection()
	if (!foundCoordinator) {
		// Broadcast to all hosts that I am the coordinator
		setIAmCoordinator()
	} 
	// If not the coordinator just send back OK
	return &proto.EmptyMessage{}, nil
}

func (s *MutualExclusionServiceServer) SetCoordinator(ctx context.Context, in *proto.ElectionResult) (*proto.EmptyMessage, error) {
	coordinator = in
	log.Printf("Coordinator set to: %s", coordinator)
	return &proto.EmptyMessage{}, nil
}

func setIAmCoordinator (){
	log.Printf("I am the coordinator!")
	coordinator = &proto.ElectionResult{Ip: IP, Port: PORT}
	for _, conn := range connections {
		conn.SetCoordinator(context.Background(), &proto.ElectionResult{Ip: IP, Port: PORT})
	} 
}