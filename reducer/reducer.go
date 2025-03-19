package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"sync"

	pb "progettoSDCC/proto"

	"google.golang.org/grpc"
)

type reducerServer struct {
	pb.UnimplementedReducerServer
	reducerID    string
	mappedChunks [][]int32
	mutex        sync.Mutex
}

func (s *reducerServer) ReceiveChunk(ctx context.Context, in *pb.MappedData) (*pb.Ack, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	log.Printf("Reducer %s received chunk for Reducer ID %d: %v", s.reducerID, in.ReducerId, in.Data)

	// Aggiungi i dati ricevuti ai chunks mappati
	s.mappedChunks = append(s.mappedChunks, in.Data)
	log.Printf("Reducer %s current mappedChunks: %v", s.reducerID, s.mappedChunks)

	return &pb.Ack{Message: "Chunk received"}, nil
}

func (s *reducerServer) StartReduction(ctx context.Context, in *pb.Empty) (*pb.Ack, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	log.Printf("Reducer %s starting reduction", s.reducerID)

	// Combina i dati ricevuti
	var combinedData []int32
	for _, chunk := range s.mappedChunks {
		combinedData = append(combinedData, chunk...)
	}

	// Ordina i dati combinati
	sort.Slice(combinedData, func(i, j int) bool {
		return combinedData[i] < combinedData[j]
	})

	// Scrivi i dati ordinati in un file
	outputFile := fmt.Sprintf("reducer_output_%s.txt", s.reducerID)
	err := writeToFile(outputFile, combinedData)
	if err != nil {
		log.Printf("Reducer %s failed to write to file: %v", s.reducerID, err)
		return &pb.Ack{Message: "Error writing to file"}, err
	}

	log.Printf("Reducer %s completed reduction and wrote to file: %s", s.reducerID, outputFile)

	// Notifica che il lavoro è completato
	return &pb.Ack{Message: "Reduction completed and data written to file"}, nil
}

// writeToFile scrive i dati ordinati in un file .txt, sovrascrivendolo all'avvio.
func writeToFile(fileName string, data []int32) error {
	// Apre il file in modalità scrittura (creandolo o troncandolo)
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Scrive i dati nel file
	for _, num := range data {
		_, err := file.WriteString(fmt.Sprintf("%d\n", num))
		if err != nil {
			return err
		}
	}

	log.Printf("Data written to file: %s", fileName)
	return nil
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

func main() {
	// Carica la configurazione dal file JSON
	config := loadConfig("config.json")

	// Crea un WaitGroup per aspettare che tutti i server siano avviati
	var wg sync.WaitGroup

	// Inizializza e avvia un server per ogni reducer
	for i, address := range config.Reducers {
		wg.Add(1) // Aggiungi un server al WaitGroup
		go func(reducerID string, address string) {
			defer wg.Done() // Decrementa il contatore quando il server è avviato

			// Inizializza il server Reducer
			lis, err := net.Listen("tcp", address)
			if err != nil {
				log.Fatalf("Failed to listen on %s: %v", address, err)
			}

			grpcServer := grpc.NewServer()
			pb.RegisterReducerServer(grpcServer, &reducerServer{
				reducerID:    reducerID,
				mappedChunks: make([][]int32, 0),
			})

			log.Printf("Reducer %s listening on %s", reducerID, address)
			if err := grpcServer.Serve(lis); err != nil {
				log.Fatalf("Failed to serve: %v", err)
			}
		}(fmt.Sprintf("reducer%d", i+1), address)
	}

	// Aspetta che tutti i server siano avviati
	wg.Wait()
}
