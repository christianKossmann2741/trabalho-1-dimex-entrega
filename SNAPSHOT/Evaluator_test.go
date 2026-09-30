package SNAPSHOT

import (
	"SD/DIMEX"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ------------------------------------------------------------------------------------
// ------- ajuda para montar cortes na mao
// ------------------------------------------------------------------------------------

func registro(id int, estado string, waiting []bool, nbrResps int, canais map[int][]string) DIMEX.SnapshotRecord {
	msgs := make(map[int][]DIMEX.ChannelMessage)
	for origem, conteudos := range canais {
		for _, c := range conteudos {
			msgs[origem] = append(msgs[origem], DIMEX.ChannelMessage{From: origem, Content: c})
		}
	}
	return DIMEX.SnapshotRecord{
		SnapshotID: 1,
		ProcessID:  id,
		Local: DIMEX.LocalState{
			ProcessID: id,
			State:     estado,
			Waiting:   waiting,
			NbrResps:  nbrResps,
		},
		Channels: msgs,
		Fault:    "none",
	}
}

func corte(registros ...DIMEX.SnapshotRecord) Cut {
	m := make(map[int]DIMEX.SnapshotRecord)
	for _, r := range registros {
		m[r.ProcessID] = r
	}
	return Cut{SnapshotID: 1, Records: m}
}

func semViolacao(t *testing.T, nome string, vs []Violation) {
	t.Helper()
	if len(vs) != 0 {
		t.Errorf("%s: esperava nenhuma violacao, veio %v", nome, vs)
	}
}

func comViolacao(t *testing.T, nome string, vs []Violation) {
	t.Helper()
	if len(vs) == 0 {
		t.Errorf("%s: esperava violacao, nao veio nenhuma", nome)
	}
}

// ------------------------------------------------------------------------------------
// ------- invariante 1: exclusao mutua
// ------------------------------------------------------------------------------------

func TestExclusaoMutua(t *testing.T) {
	nenhum := corte(
		registro(0, "noMX", []bool{false, false, false}, 0, nil),
		registro(1, "noMX", []bool{false, false, false}, 0, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, nil))
	semViolacao(t, "nenhum em inMX", CheckMutualExclusion(nenhum))

	um := corte(
		registro(0, "inMX", []bool{false, false, false}, 2, nil),
		registro(1, "noMX", []bool{false, false, false}, 0, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, nil))
	semViolacao(t, "um em inMX", CheckMutualExclusion(um))

	dois := corte(
		registro(0, "inMX", []bool{false, false, false}, 2, nil),
		registro(1, "inMX", []bool{false, false, false}, 2, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, nil))
	comViolacao(t, "dois em inMX", CheckMutualExclusion(dois))
}

// ------------------------------------------------------------------------------------
// ------- invariante 2: sistema livre
// ------------------------------------------------------------------------------------

func TestSistemaLivre(t *testing.T) {
	limpo := corte(
		registro(0, "noMX", []bool{false, false, false}, 0, nil),
		registro(1, "noMX", []bool{false, false, false}, 0, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, nil))
	semViolacao(t, "todos livres", CheckSystemFree(limpo))

	comFlag := corte(
		registro(0, "noMX", []bool{false, true, false}, 0, nil),
		registro(1, "noMX", []bool{false, false, false}, 0, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, nil))
	comViolacao(t, "flag pendurada", CheckSystemFree(comFlag))

	comMensagem := corte(
		registro(0, "noMX", []bool{false, false, false}, 0, map[int][]string{1: {"respOK:1"}}),
		registro(1, "noMX", []bool{false, false, false}, 0, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, nil))
	comViolacao(t, "mensagem em transito", CheckSystemFree(comMensagem))

	// com alguem querendo a SC a invariante nem se aplica
	naoSeAplica := corte(
		registro(0, "wantMX", []bool{false, false, false}, 0, nil),
		registro(1, "noMX", []bool{false, true, false}, 0, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, nil))
	semViolacao(t, "sistema ocupado", CheckSystemFree(naoSeAplica))
}

// ------------------------------------------------------------------------------------
// ------- invariante 3: resposta adiada valida
// ------------------------------------------------------------------------------------

func TestRespostaAdiadaValida(t *testing.T) {
	valido := corte(
		registro(0, "inMX", []bool{false, true, false}, 2, nil),
		registro(1, "wantMX", []bool{false, false, false}, 0, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, nil))
	semViolacao(t, "quem adia esta em inMX", CheckDeferredValid(valido))

	invalido := corte(
		registro(0, "noMX", []bool{false, true, false}, 0, nil),
		registro(1, "wantMX", []bool{false, false, false}, 0, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, nil))
	comViolacao(t, "quem adia esta em noMX", CheckDeferredValid(invalido))
}

// ------------------------------------------------------------------------------------
// ------- invariante 4: conservacao de respostas
// ------------------------------------------------------------------------------------

func TestConservacaoDeRespostas(t *testing.T) {
	// processo 1 quer a SC: 0 ja respondeu, 2 adiou. 1 + 0 + 1 + 0 = 2 = N-1
	certo := corte(
		registro(0, "noMX", []bool{false, false, false}, 0, nil),
		registro(1, "wantMX", []bool{false, false, false}, 1, nil),
		registro(2, "inMX", []bool{false, true, false}, 2, nil))
	semViolacao(t, "soma exata", CheckResponseConservation(certo))

	// respOK de 0 ainda viajando para 1 no lugar de ja contado
	comTransito := corte(
		registro(0, "noMX", []bool{false, false, false}, 0, nil),
		registro(1, "wantMX", []bool{false, false, false}, 0, map[int][]string{0: {"respOK:0"}}),
		registro(2, "inMX", []bool{false, true, false}, 2, nil))
	semViolacao(t, "resposta em transito conta", CheckResponseConservation(comTransito))

	// pedido de 1 ainda nem chegou em ninguem
	pedidoViajando := corte(
		registro(0, "noMX", []bool{false, false, false}, 0, map[int][]string{1: {"reqEntry:1:5"}}),
		registro(1, "wantMX", []bool{false, false, false}, 0, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, map[int][]string{1: {"reqEntry:1:5"}}))
	semViolacao(t, "pedido em transito conta", CheckResponseConservation(pedidoViajando))

	abaixo := corte(
		registro(0, "noMX", []bool{false, false, false}, 0, nil),
		registro(1, "wantMX", []bool{false, false, false}, 0, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, nil))
	comViolacao(t, "soma abaixo", CheckResponseConservation(abaixo))

	acima := corte(
		registro(0, "noMX", []bool{false, false, false}, 0, nil),
		registro(1, "wantMX", []bool{false, false, false}, 2, nil),
		registro(2, "inMX", []bool{false, true, false}, 2, nil))
	comViolacao(t, "soma acima", CheckResponseConservation(acima))
}

// ------------------------------------------------------------------------------------
// ------- invariante 5: ausencia de ciclo
// ------------------------------------------------------------------------------------

func TestAusenciaDeCiclo(t *testing.T) {
	cadeia := corte(
		registro(0, "inMX", []bool{false, true, false}, 2, nil),
		registro(1, "wantMX", []bool{false, false, true}, 0, nil),
		registro(2, "wantMX", []bool{false, false, false}, 0, nil))
	semViolacao(t, "cadeia sem ciclo", CheckNoCycle(cadeia))

	cicloDois := corte(
		registro(0, "wantMX", []bool{false, true, false}, 0, nil),
		registro(1, "wantMX", []bool{true, false, false}, 0, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, nil))
	comViolacao(t, "ciclo de dois", CheckNoCycle(cicloDois))

	cicloTres := corte(
		registro(0, "wantMX", []bool{false, true, false}, 0, nil),
		registro(1, "wantMX", []bool{false, false, true}, 0, nil),
		registro(2, "wantMX", []bool{true, false, false}, 0, nil))
	comViolacao(t, "ciclo de tres", CheckNoCycle(cicloTres))
}

// ------------------------------------------------------------------------------------
// ------- invariantes estruturais
// ------------------------------------------------------------------------------------

func TestEstruturais(t *testing.T) {
	ok := corte(
		registro(0, "inMX", []bool{false, true, false}, 2, nil),
		registro(1, "wantMX", []bool{false, false, false}, 0, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, nil))
	semViolacao(t, "registros sadios", CheckStructural(ok))

	proprio := corte(
		registro(0, "inMX", []bool{true, false, false}, 2, nil),
		registro(1, "noMX", []bool{false, false, false}, 0, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, nil))
	comViolacao(t, "waiting de si mesmo", CheckStructural(proprio))

	respsDemais := corte(
		registro(0, "wantMX", []bool{false, false, false}, 5, nil),
		registro(1, "noMX", []bool{false, false, false}, 0, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, nil))
	comViolacao(t, "nbrResps fora da faixa", CheckStructural(respsDemais))

	inMXIncompleto := corte(
		registro(0, "inMX", []bool{false, false, false}, 1, nil),
		registro(1, "noMX", []bool{false, false, false}, 0, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, nil))
	comViolacao(t, "inMX sem N-1 respostas", CheckStructural(inMXIncompleto))

	noMXComResps := corte(
		registro(0, "noMX", []bool{false, false, false}, 1, nil),
		registro(1, "noMX", []bool{false, false, false}, 0, nil),
		registro(2, "noMX", []bool{false, false, false}, 0, nil))
	comViolacao(t, "noMX com respostas", CheckStructural(noMXComResps))
}

// ------------------------------------------------------------------------------------
// ------- leitura dos arquivos
// ------------------------------------------------------------------------------------

func escreveJSONL(t *testing.T, dir string, id int, registros []DIMEX.SnapshotRecord) {
	t.Helper()
	arquivo, err := os.Create(filepath.Join(dir, "process-"+itoa(id)+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer arquivo.Close()
	cod := json.NewEncoder(arquivo)
	for _, r := range registros {
		if err := cod.Encode(r); err != nil {
			t.Fatal(err)
		}
	}
}

func itoa(n int) string {
	return strings.TrimSpace(string(rune('0' + n)))
}

func TestLoadDirLeEAgrupa(t *testing.T) {
	dir := t.TempDir()

	for id := 0; id < 2; id++ {
		var registros []DIMEX.SnapshotRecord
		for snap := 1; snap <= 3; snap++ {
			r := registro(id, "noMX", []bool{false, false}, 0, nil)
			r.SnapshotID = snap
			registros = append(registros, r)
		}
		escreveJSONL(t, dir, id, registros)
	}

	cortes, problemas, err := LoadDir(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(problemas) != 0 {
		t.Errorf("problemas inesperados: %v", problemas)
	}
	if len(cortes) != 3 {
		t.Fatalf("cortes = %d, esperado 3", len(cortes))
	}
	if cortes[0].SnapshotID != 1 || cortes[2].SnapshotID != 3 {
		t.Errorf("cortes fora de ordem: %d..%d", cortes[0].SnapshotID, cortes[2].SnapshotID)
	}
	for _, c := range cortes {
		if len(c.Records) != 2 {
			t.Errorf("corte %d tem %d registros, esperado 2", c.SnapshotID, len(c.Records))
		}
	}
}

func TestLoadDirDetectaProcessoAusente(t *testing.T) {
	dir := t.TempDir()

	r0 := registro(0, "noMX", []bool{false, false}, 0, nil)
	escreveJSONL(t, dir, 0, []DIMEX.SnapshotRecord{r0})
	escreveJSONL(t, dir, 1, nil)

	cortes, problemas, err := LoadDir(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(cortes) != 0 {
		t.Errorf("corte incompleto virou corte valido")
	}
	if len(problemas) != 1 || problemas[0].Rule != "snapshot incompleto" {
		t.Errorf("problemas = %v, esperava snapshot incompleto", problemas)
	}
}

func TestLoadDirDetectaDuplicata(t *testing.T) {
	dir := t.TempDir()

	r := registro(0, "noMX", []bool{false}, 0, nil)
	escreveJSONL(t, dir, 0, []DIMEX.SnapshotRecord{r, r})

	_, problemas, err := LoadDir(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(problemas) != 1 || problemas[0].Rule != "registro duplicado" {
		t.Errorf("problemas = %v, esperava registro duplicado", problemas)
	}
}

func TestLoadFileRecusaJSONInvalido(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "process-0.jsonl")
	if err := os.WriteFile(path, []byte("{isso nao e json}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadFile(path)
	if err == nil {
		t.Fatalf("esperava erro de JSON invalido")
	}
	if !strings.Contains(err.Error(), "JSON invalido") {
		t.Errorf("erro pouco claro: %v", err)
	}
}
