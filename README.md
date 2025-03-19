# Progetto SDCC: Sistema Distribuito per Ordinamento

Questo progetto implementa un sistema distribuito per l'ordinamento di numeri utilizzando un'architettura MapReduce. È composto da un **Master**, vari **Mapper**, e **Reducer** che collaborano per dividere i dati, ordinarli e combinarli.

## Struttura del Progetto

Il sistema è suddiviso in tre componenti principali:

1. **Master:**  
   Gestisce il coordinamento dei nodi Mapper e Reducer. Divide il dataset in chunk, assegna i range ai Reducer e avvia il processo di riduce quando tutti i Mapper hanno completato il loro lavoro.

2. **Mapper:**  
   Ordina localmente i chunk di dati ricevuti dal Master e li invia ai Reducer in base ai range specificati.

3. **Reducer:**  
   Riceve i dati ordinati dai Mapper, li combina e produce il risultato finale ordinato, tale risultato verrà poi scritto in un file .txt.

## Requisiti

- **Go 1.20 o superiore**
- **Protocol Buffers** per la definizione delle interfacce gRPC.
- Libreria `google.golang.org/grpc`.

## Configurazione

Il sistema utilizza due file principali per l'input e la configurazione:

1. **`dataset.json`:**  
   Contiene i valori numerici da ordinare. Esempio:

   ```json
   {
     "data": [45, 12, 89, 33, 78, 23, 56, 91]
   }

2. **`config.json`:**  
   Contiene indirizzi e porte dei nodi Master, Mapper, Reducer. Esempio:

   ```json
   {
     "master": "localhost:5000",
     "mappers": ["localhost:5001", "localhost:5002"],
     "reducers": ["localhost:5003", "localhost:5004"]
   }

## Istruzioni per esecuzione
1. **Crea un modulo Go:**  
   Esegui i seguenti comandi per inizializzare il modulo Go e installare le dipendenze necessarie:

   ```bash
   go mod init progettoSDCC
   go mod tidy
2. **Compila i file Proto:**  
   Genera i file necessari per gRPC utilizzando Protocol Buffers:

   ```bash
   protoc --go_out=. --go-grpc_out=. proto/rpc.proto
3. **Avvia i server Mapper:**  
   Genera i file necessari per gRPC utilizzando Protocol Buffers:

   ```bash
   go run mapper/mapper.go
4. **Avvia i server Reducer:**  
   Genera i file necessari per gRPC utilizzando Protocol Buffers:

   ```bash
   go run reducer/reducer.go
5. **Avvia il server Master:**
   ```bash
   go run master/master.go