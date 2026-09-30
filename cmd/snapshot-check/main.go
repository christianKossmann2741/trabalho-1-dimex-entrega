/*  Ferramenta offline que le os snapshots gravados e cobra as invariantes.

      go run ./cmd/snapshot-check --input /tmp/dimex-snapshots/correct --processes 3

    Sai com codigo zero somente quando nao ha nenhuma violacao.
*/

package main

import (
	"SD/SNAPSHOT"
	"flag"
	"fmt"
	"os"
	"sort"
)

func main() {
	input := flag.String("input", "", "diretorio com os arquivos process-<id>.jsonl")
	processes := flag.Int("processes", 3, "quantidade de processos")
	verbose := flag.Bool("verbose", false, "lista todas as violacoes, nao so as primeiras")
	flag.Parse()

	if *input == "" {
		fmt.Println("informe --input <diretorio>")
		os.Exit(2)
	}

	cortes, problemas, err := SNAPSHOT.LoadDir(*input, *processes)
	if err != nil {
		fmt.Println("erro lendo os snapshots:", err)
		os.Exit(2)
	}

	porRegra := make(map[string]int)
	var idsComProblema []int
	vistos := make(map[int]bool)

	for _, v := range problemas {
		porRegra[v.Rule]++
		if !vistos[v.SnapshotID] {
			vistos[v.SnapshotID] = true
			idsComProblema = append(idsComProblema, v.SnapshotID)
		}
	}

	for _, corte := range cortes {
		for _, v := range SNAPSHOT.CheckCut(corte) {
			problemas = append(problemas, v)
			porRegra[v.Rule]++
			if !vistos[v.SnapshotID] {
				vistos[v.SnapshotID] = true
				idsComProblema = append(idsComProblema, v.SnapshotID)
			}
		}
	}

	fmt.Printf("diretorio     : %s\n", *input)
	fmt.Printf("processos     : %d\n", *processes)
	fmt.Printf("cortes completos: %d\n", len(cortes))
	if len(cortes) > 0 {
		fmt.Printf("faixa de ids  : %d a %d\n", cortes[0].SnapshotID, cortes[len(cortes)-1].SnapshotID)
	}
	fmt.Printf("violacoes     : %d\n", len(problemas))

	if len(problemas) == 0 {
		fmt.Println("\nnenhuma invariante foi violada")
		return
	}

	var regras []string
	for r := range porRegra {
		regras = append(regras, r)
	}
	sort.Strings(regras)

	fmt.Println("\npor regra:")
	for _, r := range regras {
		fmt.Printf("  %-28s %d\n", r, porRegra[r])
	}

	sort.Ints(idsComProblema)
	fmt.Printf("\nsnapshots com violacao (%d): %v\n", len(idsComProblema), primeiros(idsComProblema, 40))

	limite := len(problemas)
	if !*verbose && limite > 20 {
		limite = 20
	}
	fmt.Println("\ndetalhe:")
	for _, v := range problemas[:limite] {
		fmt.Println("  " + v.String())
	}
	if limite < len(problemas) {
		fmt.Printf("  ... mais %d (use --verbose)\n", len(problemas)-limite)
	}

	os.Exit(1)
}

func primeiros(ids []int, n int) []int {
	if len(ids) <= n {
		return ids
	}
	return ids[:n]
}
