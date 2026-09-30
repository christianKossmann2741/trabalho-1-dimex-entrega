/*  Avaliador offline dos snapshots do DiMEx.

    Le os arquivos process-<id>.jsonl gerados por cada processo, junta os
    registros de um mesmo SnapshotID num corte consistente e testa as
    invariantes do algoritmo de exclusao mutua sobre esse corte.
*/

package SNAPSHOT

import (
	"SD/DIMEX"
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Violation e uma invariante que nao passou num snapshot.
type Violation struct {
	SnapshotID int
	Rule       string
	Detail     string
}

func (v Violation) String() string {
	return fmt.Sprintf("snapshot %d: %s: %s", v.SnapshotID, v.Rule, v.Detail)
}

// Cut e o conjunto de registros de um snapshot, indexado pelo id do processo.
type Cut struct {
	SnapshotID int
	Records    map[int]DIMEX.SnapshotRecord
}

// ------------------------------------------------------------------------------------
// ------- leitura dos arquivos
// ------------------------------------------------------------------------------------

// LoadFile le um process-<id>.jsonl inteiro.
func LoadFile(path string) ([]DIMEX.SnapshotRecord, error) {
	arquivo, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer arquivo.Close()

	var registros []DIMEX.SnapshotRecord
	scanner := bufio.NewScanner(arquivo)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	linha := 0
	for scanner.Scan() {
		linha++
		texto := strings.TrimSpace(scanner.Text())
		if texto == "" {
			continue
		}
		var rec DIMEX.SnapshotRecord
		if err := json.Unmarshal([]byte(texto), &rec); err != nil {
			return nil, fmt.Errorf("%s linha %d: JSON invalido: %v", path, linha, err)
		}
		registros = append(registros, rec)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return registros, nil
}

// LoadDir le os arquivos dos n processos e agrupa tudo por SnapshotID.
func LoadDir(dir string, n int) ([]Cut, []Violation, error) {
	porSnapshot := make(map[int]map[int]DIMEX.SnapshotRecord)
	var problemas []Violation

	for id := 0; id < n; id++ {
		path := filepath.Join(dir, fmt.Sprintf("process-%d.jsonl", id))
		registros, err := LoadFile(path)
		if err != nil {
			return nil, nil, err
		}
		for _, rec := range registros {
			if rec.ProcessID != id {
				return nil, nil, fmt.Errorf("%s contem registro do processo %d", path, rec.ProcessID)
			}
			if porSnapshot[rec.SnapshotID] == nil {
				porSnapshot[rec.SnapshotID] = make(map[int]DIMEX.SnapshotRecord)
			}
			if _, repetido := porSnapshot[rec.SnapshotID][id]; repetido {
				problemas = append(problemas, Violation{rec.SnapshotID, "registro duplicado",
					fmt.Sprintf("processo %d gravou o snapshot %d mais de uma vez", id, rec.SnapshotID)})
				continue
			}
			porSnapshot[rec.SnapshotID][id] = rec
		}
	}

	var ids []int
	for id := range porSnapshot {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	var cortes []Cut
	for _, id := range ids {
		registros := porSnapshot[id]
		if len(registros) != n {
			var faltando []int
			for p := 0; p < n; p++ {
				if _, ok := registros[p]; !ok {
					faltando = append(faltando, p)
				}
			}
			problemas = append(problemas, Violation{id, "snapshot incompleto",
				fmt.Sprintf("faltam os processos %v", faltando)})
			continue
		}
		cortes = append(cortes, Cut{SnapshotID: id, Records: registros})
	}

	return cortes, problemas, nil
}

// ------------------------------------------------------------------------------------
// ------- ajuda
// ------------------------------------------------------------------------------------

// inTransit conta mensagens de um tipo que estavam viajando para dest.
func inTransit(cut Cut, dest int, kind string) int {
	n := 0
	for _, msgs := range cut.Records[dest].Channels {
		for _, m := range msgs {
			if strings.HasPrefix(m.Content, kind+":") {
				n++
			}
		}
	}
	return n
}

// inTransitFrom conta mensagens de um tipo enviadas por origem, para qualquer destino.
func inTransitFrom(cut Cut, origem int, kind string) int {
	n := 0
	for _, rec := range cut.Records {
		for _, msgs := range rec.Channels {
			for _, m := range msgs {
				if m.From == origem && strings.HasPrefix(m.Content, kind+":") {
					n++
				}
			}
		}
	}
	return n
}

// ------------------------------------------------------------------------------------
// ------- invariantes
// ------------------------------------------------------------------------------------

// CheckMutualExclusion: no maximo um processo dentro da secao critica.
func CheckMutualExclusion(cut Cut) []Violation {
	var dentro []int
	for id, rec := range cut.Records {
		if rec.Local.State == "inMX" {
			dentro = append(dentro, id)
		}
	}
	if len(dentro) > 1 {
		sort.Ints(dentro)
		return []Violation{{cut.SnapshotID, "exclusao mutua",
			fmt.Sprintf("processos %v estao em inMX ao mesmo tempo", dentro)}}
	}
	return nil
}

// CheckSystemFree: se ninguem quer a SC, nao pode sobrar flag nem mensagem.
func CheckSystemFree(cut Cut) []Violation {
	for _, rec := range cut.Records {
		if rec.Local.State != "noMX" {
			return nil
		}
	}

	var problemas []Violation
	for id, rec := range cut.Records {
		for q, esperando := range rec.Local.Waiting {
			if esperando {
				problemas = append(problemas, Violation{cut.SnapshotID, "sistema livre",
					fmt.Sprintf("todos em noMX mas o processo %d ainda adia o %d", id, q)})
			}
		}
		if n := inTransit(cut, id, "reqEntry") + inTransit(cut, id, "respOK"); n > 0 {
			problemas = append(problemas, Violation{cut.SnapshotID, "sistema livre",
				fmt.Sprintf("todos em noMX mas ha %d mensagens em transito para o processo %d", n, id)})
		}
	}
	return problemas
}

// CheckDeferredValid: so adia quem quer ou esta na SC.
func CheckDeferredValid(cut Cut) []Violation {
	var problemas []Violation
	for id, rec := range cut.Records {
		for q, esperando := range rec.Local.Waiting {
			if !esperando {
				continue
			}
			if rec.Local.State != "wantMX" && rec.Local.State != "inMX" {
				problemas = append(problemas, Violation{cut.SnapshotID, "resposta adiada valida",
					fmt.Sprintf("processo %d adia o %d mas esta em %s", id, q, rec.Local.State)})
			}
		}
	}
	return problemas
}

// CheckResponseConservation: cada um dos N-1 pares de um processo em wantMX esta
// em exatamente um estagio: ja respondeu (contado em nbrResps), respondeu e a
// resposta viaja, adiou o pedido, ou ainda nem recebeu o pedido.
func CheckResponseConservation(cut Cut) []Violation {
	n := len(cut.Records)
	var problemas []Violation

	for q, rec := range cut.Records {
		if rec.Local.State != "wantMX" {
			continue
		}

		adiando := 0
		for p, outro := range cut.Records {
			if p != q && q < len(outro.Local.Waiting) && outro.Local.Waiting[q] {
				adiando++
			}
		}
		respostasViajando := inTransit(cut, q, "respOK")
		pedidosViajando := inTransitFrom(cut, q, "reqEntry")

		soma := rec.Local.NbrResps + respostasViajando + adiando + pedidosViajando
		if soma != n-1 {
			problemas = append(problemas, Violation{cut.SnapshotID, "conservacao de respostas",
				fmt.Sprintf("processo %d em wantMX: nbrResps=%d + respOK em transito=%d + adiando=%d + reqEntry em transito=%d = %d, esperado %d",
					q, rec.Local.NbrResps, respostasViajando, adiando, pedidosViajando, soma, n-1)})
		}
	}
	return problemas
}

// CheckNoCycle: o grafo de espera nao pode ter ciclo.
// Aresta q -> p quando p adia o pedido de q.
func CheckNoCycle(cut Cut) []Violation {
	n := len(cut.Records)
	adjacencia := make(map[int][]int)
	for p, rec := range cut.Records {
		for q, esperando := range rec.Local.Waiting {
			if esperando && q != p {
				adjacencia[q] = append(adjacencia[q], p)
			}
		}
	}

	cor := make(map[int]int) // 0 novo, 1 na pilha, 2 pronto
	var caminho []int
	var ciclo []int

	var visita func(int) bool
	visita = func(v int) bool {
		cor[v] = 1
		caminho = append(caminho, v)
		for _, w := range adjacencia[v] {
			if cor[w] == 1 {
				inicio := 0
				for i, x := range caminho {
					if x == w {
						inicio = i
						break
					}
				}
				ciclo = append(append([]int{}, caminho[inicio:]...), w)
				return true
			}
			if cor[w] == 0 && visita(w) {
				return true
			}
		}
		caminho = caminho[:len(caminho)-1]
		cor[v] = 2
		return false
	}

	for v := 0; v < n; v++ {
		if cor[v] == 0 && visita(v) {
			return []Violation{{cut.SnapshotID, "ausencia de ciclo",
				fmt.Sprintf("espera circular: %v", ciclo)}}
		}
	}
	return nil
}

// CheckStructural junta as checagens de sanidade de cada registro.
func CheckStructural(cut Cut) []Violation {
	n := len(cut.Records)
	var problemas []Violation

	for id, rec := range cut.Records {
		if id < len(rec.Local.Waiting) && rec.Local.Waiting[id] {
			problemas = append(problemas, Violation{cut.SnapshotID, "estrutural",
				fmt.Sprintf("processo %d marca a si mesmo em waiting", id)})
		}
		if rec.Local.NbrResps < 0 || rec.Local.NbrResps > n-1 {
			problemas = append(problemas, Violation{cut.SnapshotID, "estrutural",
				fmt.Sprintf("processo %d tem nbrResps=%d fora de [0, %d]", id, rec.Local.NbrResps, n-1)})
		}
		if rec.Local.State == "inMX" && rec.Local.NbrResps != n-1 {
			problemas = append(problemas, Violation{cut.SnapshotID, "estrutural",
				fmt.Sprintf("processo %d esta em inMX com nbrResps=%d, esperado %d", id, rec.Local.NbrResps, n-1)})
		}
		if rec.Local.State == "noMX" && rec.Local.NbrResps != 0 {
			problemas = append(problemas, Violation{cut.SnapshotID, "estrutural",
				fmt.Sprintf("processo %d esta em noMX com nbrResps=%d, esperado 0", id, rec.Local.NbrResps)})
		}
	}
	return problemas
}

// CheckCut roda todas as invariantes sobre um corte.
func CheckCut(cut Cut) []Violation {
	var problemas []Violation
	problemas = append(problemas, CheckMutualExclusion(cut)...)
	problemas = append(problemas, CheckSystemFree(cut)...)
	problemas = append(problemas, CheckDeferredValid(cut)...)
	problemas = append(problemas, CheckResponseConservation(cut)...)
	problemas = append(problemas, CheckNoCycle(cut)...)
	problemas = append(problemas, CheckStructural(cut)...)
	return problemas
}
