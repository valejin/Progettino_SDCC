package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"os"
	"sync"

	pb "progettoSDCC/proto"

	"google.golang.org/grpc"
)

type masterServer struct {
	pb.UnimplementedMasterServer
	mappersCompleted map[string]bool
	mutex            sync.Mutex
	reducers         []string
	mappers          []string
	masterAddr       string
	ranges           [][2]int32
}

// Metodo che calcola i range per i reducer
func calculateRanges(maxValue int32, numReducers int) [][2]int32 {
	var ranges [][2]int32
	chunkSize := maxValue / int32(numReducers)

	for i := 0; i < numReducers; i++ {
		start := int32(i) * chunkSize
		end := start + chunkSize - 1
		if i == numReducers-1 {
			end = maxValue // L'ultimo range deve includere il massimo valore
		}
		ranges = append(ranges, [2]int32{start, end})
	}
	return ranges
}

// Metodo che distribuisce i dati e i range ai mappers
func (s *masterServer) distributeDataToMappers() {
	// Carica il dataset dal file
	dataset := loadDataset("dataset.json")

	// Invio dei range ai mappers
	for _, mapperAddr := range s.mappers {
		log.Printf("Sending ranges to mapper %s: %v", mapperAddr, s.ranges)

		// Connessione al mapper
		conn, err := grpc.Dial(mapperAddr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Could not connect to mapper: %v", err)
		}
		defer conn.Close()

		client := pb.NewMapperClient(conn)

		// Invio dei range al mapper
		_, err = client.SendRanges(context.Background(), &pb.RangeList{Ranges: convertRangesToProto(s.ranges)})
		if err != nil {
			log.Printf("Failed to send ranges to mapper %s: %v", mapperAddr, err)
		}
	}

	// Suddivisione del dataset in chunk per i mappers
	chunkSize := len(dataset) / len(s.mappers)
	for i, mapperAddr := range s.mappers {
		chunk := dataset[i*chunkSize : (i+1)*chunkSize]
		log.Printf("Distributing chunk to mapper %s: %v", mapperAddr, chunk)

		// Connessione al mapper
		conn, err := grpc.Dial(mapperAddr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Could not connect to mapper: %v", err)
		}
		defer conn.Close()

		client := pb.NewMapperClient(conn)

		// Invio del chunk di dati al mapper
		_, err = client.ProcessChunk(context.Background(), &pb.DataChunk{Data: chunk})
		if err != nil {
			log.Printf("Failed to send chunk to mapper %s: %v", mapperAddr, err)
		}
	}
}

// Metodo per convertire i range in un formato compatibile con il proto
func convertRangesToProto(ranges [][2]int32) []*pb.Range {
	var protoRanges []*pb.Range
	for _, r := range ranges {
		protoRanges = append(protoRanges, &pb.Range{Start: r[0], End: r[1]})
	}
	return protoRanges
}

// Metodo per notificare il completamento di un mapper
func (s *masterServer) NotifyMapperCompletion(ctx context.Context, in *pb.MapperStatus) (*pb.Ack, error) {
	log.Println("Received notification of mapper completion")
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.mappersCompleted[in.MapperId] = true
	log.Printf("Mapper %s completed processing", in.MapperId)

	if len(s.mappersCompleted) == len(s.mappers) {
		log.Println("All mappers completed, starting reduction")

		for _, reducerAddr := range s.reducers {
			log.Printf("Creating connection with reducer at %s", reducerAddr)

			conn, err := grpc.Dial(reducerAddr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Could not connect to reducer: %v", err)
			}
			defer conn.Close()

			client := pb.NewReducerClient(conn)
			_, err = client.StartReduction(context.Background(), &pb.Empty{})
			if err != nil {
				log.Printf("Failed to signal reducer %s: %v", reducerAddr, err)
			}
		}
		log.Println("All reducers signaled to start reduction")
	} else {
		log.Println("Not all mappers have completed yet")
	}

	return &pb.Ack{Message: "Mapper completion noted"}, nil
}

// Struttura per il dataset
type Dataset struct {
	Data []int32 `json:"data"`
}

// Funzione per caricare il dataset dal file JSON
func loadDataset(filePath string) []int32 {
	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Failed to read dataset: %v", err)
	}

	var dataset Dataset
	if err := json.Unmarshal(data, &dataset); err != nil {
		log.Fatalf("Failed to unmarshal dataset: %v", err)
	}

	return dataset.Data
}

// Struttura per la configurazione
type Config struct {
	Master   string   `json:"master"`
	Mappers  []string `json:"mappers"`
	Reducers []string `json:"reducers"`
}

// Funzione per caricare la configurazione dal file JSON
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

func main() {
	// Carica la configurazione dal file JSON
	config := loadConfig("config.json")

	// Carica il dataset e calcola i range per i reducer
	dataset := loadDataset("dataset.json")
	maxValue := int32(0)
	for _, value := range dataset {
		if value > maxValue {
			maxValue = value
		}
	}

	ranges := calculateRanges(maxValue, len(config.Reducers))

	// Imposta il listener per il server gRPC
	lis, err := net.Listen("tcp", config.Master)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", config.Master, err)
	}

	// Crea il server gRPC
	master := &masterServer{
		mappersCompleted: make(map[string]bool),
		reducers:         config.Reducers,
		mappers:          config.Mappers,
		masterAddr:       config.Master,
		ranges:           ranges,
	}
	grpcServer := grpc.NewServer()
	pb.RegisterMasterServer(grpcServer, master)

	log.Printf("Master listening on %s", config.Master)

	// Avvia la distribuzione dei dati
	go master.distributeDataToMappers()

	// Avvia il server gRPC
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
