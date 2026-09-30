/*  Aplicacao que usa o DiMEx e grava snapshots de Chandy-Lamport.

    Cada processo escreve seus registros em <output>/process-<id>.jsonl, um JSON
    por linha, e todos disputam o mesmo <output>/mxOUT.txt pelo protocolo. Se o
    DiMEx estiver correto o arquivo compartilhado so tem "|." repetido.

    Exemplo com tres processos:
      go run ./cmd/dimex-snapshot --id 0 --initiator --snapshots 300 --output /tmp/x \
          127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002
*/

package main

import (
	"SD/DIMEX"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	id := flag.Int("id", 0, "id deste processo, indice na lista de enderecos")
	initiator := flag.Bool("initiator", false, "este processo inicia os snapshots")
	snapshots := flag.Int("snapshots", 0, "quantos snapshots o iniciador dispara")
	interval := flag.Duration("interval", 30*time.Millisecond, "intervalo entre snapshots")
	fault := flag.String("fault", "none", "modo de falha: none, mutex ou deadlock")
	output := flag.String("output", "./runs", "diretorio de saida")
	duration := flag.Duration("duration", 0, "tempo de execucao; 0 roda ate receber sinal")
	flag.Parse()

	addresses := flag.Args()
	if len(addresses) == 0 {
		fmt.Println("faltam os enderecos dos processos")
		fmt.Println("uso: dimex-snapshot --id 0 [flags] 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002")
		os.Exit(1)
	}
	if *id < 0 || *id >= len(addresses) {
		fmt.Printf("id %d fora da lista de %d enderecos\n", *id, len(addresses))
		os.Exit(1)
	}

	modoFalha, err := DIMEX.ParseFaultMode(*fault)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := os.MkdirAll(*output, 0o755); err != nil {
		fmt.Println("erro criando diretorio de saida:", err)
		os.Exit(1)
	}

	arquivoSnap, err := os.Create(filepath.Join(*output, fmt.Sprintf("process-%d.jsonl", *id)))
	if err != nil {
		fmt.Println("erro criando arquivo de snapshots:", err)
		os.Exit(1)
	}
	defer arquivoSnap.Close()

	arquivoMX, err := os.OpenFile(filepath.Join(*output, "mxOUT.txt"),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Println("erro abrindo mxOUT.txt:", err)
		os.Exit(1)
	}
	defer arquivoMX.Close()

	fmt.Printf("processo %d, falha %s, saida em %s\n", *id, modoFalha, *output)

	dmx := DIMEX.NewDIMEXWithFault(addresses, *id, false, modoFalha)

	parar := make(chan struct{})
	gravados := make(chan int, 1)

	// grava cada snapshot completo assim que o modulo entrega
	go func() {
		codificador := json.NewEncoder(arquivoSnap)
		total := 0
		for {
			select {
			case rec := <-dmx.SnapshotInd:
				if err := codificador.Encode(rec); err != nil {
					fmt.Println("erro gravando snapshot:", err)
				}
				total++
			case <-parar:
				// esvazia o que ainda estiver na fila antes de sair
				for {
					select {
					case rec := <-dmx.SnapshotInd:
						codificador.Encode(rec)
						total++
					default:
						gravados <- total
						return
					}
				}
			}
		}
	}()

	// espera os outros processos subirem
	time.Sleep(2 * time.Second)

	if *initiator && *snapshots > 0 {
		go func() {
			for i := 1; i <= *snapshots; i++ {
				select {
				case dmx.SnapshotReq <- i:
				case <-parar:
					return
				}
				time.Sleep(*interval)
			}
		}()
	}

	sinais := make(chan os.Signal, 1)
	signal.Notify(sinais, os.Interrupt, syscall.SIGTERM)

	go func() {
		if *duration > 0 {
			select {
			case <-time.After(*duration):
			case <-sinais:
			}
		} else {
			<-sinais
		}
		close(parar)
	}()

	// laco intensivo de secao critica
	acessos := 0
	for {
		select {
		case <-parar:
			fmt.Printf("processo %d encerrando: %d acessos, %d snapshots gravados\n",
				*id, acessos, <-gravados)
			return
		default:
		}

		dmx.Req <- DIMEX.ENTER

		// no modo deadlock a liberacao nunca chega, entao a espera tambem
		// precisa enxergar o pedido de encerramento
		select {
		case <-dmx.Ind:
		case <-parar:
			fmt.Printf("processo %d encerrando bloqueado na espera da SC: %d acessos, %d snapshots gravados\n",
				*id, acessos, <-gravados)
			return
		}

		arquivoMX.WriteString("|")
		arquivoMX.WriteString(".")
		acessos++

		dmx.Req <- DIMEX.EXIT
	}
}
