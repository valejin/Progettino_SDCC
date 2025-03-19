package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"

	pb "progettoSDCC/proto"

	"google.golang.org/grpc"
)

type mapperServer struct {
	pb.UnimplementedMapperServer
	mapperID   string
	reducers   []string
	ranges     []pb.Range
	masterAddr string
}

func (s *mapperServer) SendRanges(ctx context.Context, in *pb.RangeList) (*pb.Ack, error) {
	// Converte []*Range in []pb.Range
	ranges := make([]pb.Range, len(in.Ranges))
	for i, r := range in.Ranges {
		ranges[i] = *r
	}
	s.ranges = ranges

	log.Printf("Mapper %s received ranges: %v", s.mapperID, s.ranges)
	return &pb.Ack{Message: "Ranges received successfully"}, nil
}

func (s *mapperServer) ProcessChunk(ctx context.Context, in *pb.DataChunk) (*pb.Ack, error) {
	log.Printf("Mapper %s received chunk: %v", s.mapperID, in.Data)

	// Ordina i dati
	sort.Slice(in.Data, func(i, j int) bool {
		return in.Data[i] < in.Data[j]
	})

	// Divide i dati per i reducer
	chunks := partitionData(in.Data, s.ranges)

	// Invia i dati ordinati ai reducer
	for i, reducerAddr := range s.reducers {
		conn, err := grpc.Dial(reducerAddr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Could not connect to reducer %s: %v", reducerAddr, err)
		}
		defer conn.Close()

		client := pb.NewReducerClient(conn)
		_, err = client.ReceiveChunk(context.Background(), &pb.MappedData{
			Data:      chunks[i],
			ReducerId: int32(i),
		})
		if err != nil {
			log.Printf("Failed to send data to reducer %s: %v", reducerAddr, err)
		}
	}

	// Connetti al master usando l'indirizzo passato (masterAddr)
	conn, err := grpc.Dial(s.masterAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect to master: %v", err)
	} else {
		log.Println("Successfully connected to master")
	}
	defer conn.Close()

	masterClient := pb.NewMasterClient(conn)

	log.Println("Notifying master about mapper completion")
	_, err = masterClient.NotifyMapperCompletion(context.Background(), &pb.MapperStatus{MapperId: s.mapperID})
	if err != nil {
		log.Printf("Error notifying master about mapper completion: %v", err)
	}

	return &pb.Ack{Message: "Chunk processed"}, nil
}

func partitionData(data []int32, ranges []pb.Range) [][]int32 {
	partitions := make([][]int32, len(ranges))
	for _, value := range data {
		for i, r := range ranges {
			if value >= r.Start && value <= r.End {
				partitions[i] = append(partitions[i], value)
				break
			}
		}
	}
	return partitions
}

type Config struct {
	Master   string   `json:"master"`
	Mappers  []string `json:"mappers"`
	Reducers []string `json:"reducers"`
}

func loadConfig(filePath string) Config {
	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Failed to read config: %v", err)
	}
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		log.Fatalf("Failed to unmarshal config: %v", err)
	}
	return config
}

func startMapperServer(mapperID string, address string, reducers []string, masterAddr string, wg *sync.WaitGroup) {
	defer wg.Done()

	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", address, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMapperServer(grpcServer, &mapperServer{
		mapperID:   mapperID,
		reducers:   reducers,
		ranges:     []pb.Range{},
		masterAddr: masterAddr,
	})

	log.Printf("Mapper %s listening on %s", mapperID, address)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func main() {
	// Carica la configurazione dal file JSON
	config := loadConfig("config.json")

	var wg sync.WaitGroup

	// Itero su tutti i mappers nel file di configurazione e avvia un server per ciascuno
	for i, mapperAddr := range config.Mappers {
		mapperID := "mapper" + strconv.Itoa(i+1) // Genera un ID unico per ogni mapper
		wg.Add(1)
		go startMapperServer(mapperID, mapperAddr, config.Reducers, config.Master, &wg) // Passa config.Master come masterAddr
	}

	// Attende che tutti i server Mapper finiscano
	wg.Wait()
}
