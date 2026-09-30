package DIMEX

import (
	PP2PLink "SD/PP2PLink"
	"fmt"
	"testing"
)

func TestFormatMarker(t *testing.T) {
	got := formatMarker(3, 1)
	esperado := "marker:3:1"

	if got != esperado {
		t.Errorf("formatMarker(3, 1) = %q, esperado %q", got, esperado)
	}
}

func TestParseMarker(t *testing.T) {
	snapID, senderID, err := parseMarker("marker:3:1")

	if err != nil {
		t.Errorf("parseMarker(\"marker:3:1\") retornou erro: %v", err)
	}
	if snapID != 3 {
		t.Errorf("snapID = %d, esperado 3", snapID)
	}
	if senderID != 1 {
		t.Errorf("senderID = %d, esperado 1", senderID)
	}
}

func TestParseMarkerInvalido(t *testing.T) {
	casos := []struct {
		nome     string
		mensagem string
	}{
		{"mensagem incompleta", "marker:3"},
		{"partes demais", "marker:3:1:9"},
		{"prefixo errado", "respOK:3:1"},
		{"snapshot nao numerico", "marker:x:1"},
		{"remetente nao numerico", "marker:3:y"},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			_, _, err := parseMarker(c.mensagem)
			if err == nil {
				t.Errorf("parseMarker(%q) deveria retornar erro, mas retornou nil", c.mensagem)
			}
		})
	}
}

// ------------------------------------------------------------------------------------
// ------- ajuda para os testes
// ------------------------------------------------------------------------------------

// moduloDeTeste monta um DIMEX_Module sem abrir sockets. O PP2PLink e so um par
// de canais com folga, entao da para ler o que o modulo tentou enviar.
func moduloDeTeste(n, id int) *DIMEX_Module {
	enderecos := make([]string, n)
	for i := range enderecos {
		enderecos[i] = fmt.Sprintf("127.0.0.1:%d", 9000+i)
	}

	return &DIMEX_Module{
		Req:           make(chan dmxReq, 1),
		Ind:           make(chan dmxResp, 8),
		addresses:     enderecos,
		id:            id,
		st:            noMX,
		waiting:       make([]bool, n),
		SnapshotReq:   make(chan int, 8),
		SnapshotInd:   make(chan SnapshotRecord, 8),
		snapshots:     make(map[int]*activeSnapshot),
		snapshotsDone: make(map[int]bool),
		fault:         FaultNone,
		Pp2plink: &PP2PLink.PP2PLink{
			Req: make(chan PP2PLink.PP2PLink_Req_Message, 64),
			Ind: make(chan PP2PLink.PP2PLink_Ind_Message, 64)},
	}
}

// enviadas esvazia a fila de saida e devolve o que foi mandado.
func enviadas(module *DIMEX_Module) []string {
	var msgs []string
	for {
		select {
		case m := <-module.Pp2plink.Req:
			msgs = append(msgs, m.Message)
		default:
			return msgs
		}
	}
}

func contaMarcadores(msgs []string) int {
	n := 0
	for _, m := range msgs {
		if messageKind(m) == "marker" {
			n++
		}
	}
	return n
}

// ------------------------------------------------------------------------------------
// ------- roteamento por prefixo
// ------------------------------------------------------------------------------------

func TestMessageKind(t *testing.T) {
	casos := []struct {
		mensagem string
		esperado string
	}{
		{"marker:1:0", "marker"},
		{"respOK:2", "respOK"},
		{"reqEntry:2:11", "reqEntry"},
		{"lixo respOK:2", ""},
		{"algo com reqEntry no meio", ""},
		{"", ""},
	}

	for _, c := range casos {
		if got := messageKind(c.mensagem); got != c.esperado {
			t.Errorf("messageKind(%q) = %q, esperado %q", c.mensagem, got, c.esperado)
		}
	}
}

// ------------------------------------------------------------------------------------
// ------- captura do estado local
// ------------------------------------------------------------------------------------

func TestCaptureLocalStateCongelaWaiting(t *testing.T) {
	module := moduloDeTeste(3, 0)
	module.st = wantMX
	module.lcl = 7
	module.reqTs = 7
	module.nbrResps = 1
	module.waiting[1] = true

	foto := module.captureLocalState()

	// mexe no modulo depois da foto
	module.waiting[1] = false
	module.waiting[2] = true
	module.lcl = 99

	if foto.State != "wantMX" {
		t.Errorf("State = %q, esperado wantMX", foto.State)
	}
	if foto.Lcl != 7 {
		t.Errorf("Lcl = %d, esperado 7", foto.Lcl)
	}
	if !foto.Waiting[1] {
		t.Errorf("Waiting[1] = false, a foto deveria ter congelado true")
	}
	if foto.Waiting[2] {
		t.Errorf("Waiting[2] = true, a foto nao deveria ver alteracao posterior")
	}
}

// ------------------------------------------------------------------------------------
// ------- inicio e marcadores
// ------------------------------------------------------------------------------------

func TestSnapshotReqCapturaAntesDeEnviarMarcador(t *testing.T) {
	module := moduloDeTeste(3, 0)
	module.st = inMX
	module.nbrResps = 2

	module.handleUponSnapshotReq(1)

	snap := module.snapshots[1]
	if snap == nil {
		t.Fatalf("snapshot 1 nao foi criado")
	}
	if snap.local.State != "inMX" || snap.local.NbrResps != 2 {
		t.Errorf("estado congelado errado: %+v", snap.local)
	}
	if n := contaMarcadores(enviadas(module)); n != 2 {
		t.Errorf("marcadores enviados = %d, esperado 2", n)
	}
}

func TestSnapshotReqIgnoraIdRepetidoOuConcluido(t *testing.T) {
	module := moduloDeTeste(3, 0)

	module.handleUponSnapshotReq(1)
	enviadas(module)

	module.handleUponSnapshotReq(1) // ja em andamento
	if n := contaMarcadores(enviadas(module)); n != 0 {
		t.Errorf("id em andamento reenviou %d marcadores", n)
	}

	module.snapshotsDone[5] = true
	module.handleUponSnapshotReq(5) // ja concluido
	if module.snapshots[5] != nil {
		t.Errorf("id ja concluido foi reaberto")
	}

	module.handleUponSnapshotReq(0) // id invalido
	if module.snapshots[0] != nil {
		t.Errorf("id invalido foi aceito")
	}
}

func TestSnapshotComUmProcessoCompletaNaHora(t *testing.T) {
	module := moduloDeTeste(1, 0)

	module.handleUponSnapshotReq(1)

	select {
	case rec := <-module.SnapshotInd:
		if rec.SnapshotID != 1 || rec.ProcessID != 0 {
			t.Errorf("registro errado: %+v", rec)
		}
	default:
		t.Errorf("snapshot com um processo so nao foi entregue")
	}
}

func TestPrimeiroMarcadorCriaSnapshotEEncaminha(t *testing.T) {
	module := moduloDeTeste(3, 0)

	module.handleUponDeliverMarker(formatMarker(1, 1))

	snap := module.snapshots[1]
	if snap == nil {
		t.Fatalf("primeiro marcador nao criou o snapshot")
	}
	if snap.recording[1] {
		t.Errorf("canal do remetente deveria estar fechado")
	}
	if !snap.recording[2] {
		t.Errorf("canal do processo 2 deveria continuar gravando")
	}
	if n := contaMarcadores(enviadas(module)); n != 2 {
		t.Errorf("marcadores encaminhados = %d, esperado 2 (N-1)", n)
	}
}

func TestMarcadorPosteriorNaoReencaminhaNemRecaptura(t *testing.T) {
	module := moduloDeTeste(3, 0)

	module.handleUponDeliverMarker(formatMarker(1, 1))
	enviadas(module)
	lclCongelado := module.snapshots[1].local.Lcl

	module.lcl = 42
	module.handleUponDeliverMarker(formatMarker(1, 2))

	if n := contaMarcadores(enviadas(module)); n != 0 {
		t.Errorf("marcador posterior reenviou %d marcadores", n)
	}
	// o snapshot ja saiu do mapa, entao confere pelo registro entregue
	rec := <-module.SnapshotInd
	if rec.Local.Lcl != lclCongelado {
		t.Errorf("estado local foi recapturado: %d, esperado %d", rec.Local.Lcl, lclCongelado)
	}
}

func TestSoCompletaComTodosOsCanaisFechados(t *testing.T) {
	module := moduloDeTeste(4, 0) // tres canais de entrada

	module.handleUponDeliverMarker(formatMarker(1, 1))
	module.handleUponDeliverMarker(formatMarker(1, 2))

	select {
	case rec := <-module.SnapshotInd:
		t.Fatalf("completou cedo demais: %+v", rec)
	default:
	}

	module.handleUponDeliverMarker(formatMarker(1, 3))

	select {
	case rec := <-module.SnapshotInd:
		if rec.SnapshotID != 1 {
			t.Errorf("registro com id errado: %d", rec.SnapshotID)
		}
	default:
		t.Errorf("ultimo marcador nao completou o snapshot")
	}
}

func TestMarcadorRepetidoNaoEntregaDuasVezes(t *testing.T) {
	module := moduloDeTeste(2, 0)

	module.handleUponDeliverMarker(formatMarker(1, 1))
	module.handleUponDeliverMarker(formatMarker(1, 1))

	if n := len(module.SnapshotInd); n != 1 {
		t.Errorf("registros entregues = %d, esperado 1", n)
	}
}

// ------------------------------------------------------------------------------------
// ------- gravacao dos canais
// ------------------------------------------------------------------------------------

func TestGravaMensagemEnquantoCanalAberto(t *testing.T) {
	module := moduloDeTeste(3, 0)
	module.handleUponSnapshotReq(1)

	module.recordInTransit(formatReqEntry(1, 5))
	module.recordInTransit(formatRespOK(2))

	snap := module.snapshots[1]
	if len(snap.messages[1]) != 1 || snap.messages[1][0].Content != "reqEntry:1:5" {
		t.Errorf("canal 1 gravou %+v", snap.messages[1])
	}
	if len(snap.messages[2]) != 1 || snap.messages[2][0].Content != "respOK:2" {
		t.Errorf("canal 2 gravou %+v", snap.messages[2])
	}
}

func TestMensagemDepoisDoMarcadorNaoEGravada(t *testing.T) {
	module := moduloDeTeste(3, 0)
	module.handleUponSnapshotReq(1)

	module.handleUponDeliverMarker(formatMarker(1, 1)) // fecha o canal 1
	module.recordInTransit(formatRespOK(1))            // chegou depois do marcador
	module.recordInTransit(formatRespOK(2))            // canal 2 ainda aberto

	snap := module.snapshots[1]
	if len(snap.messages[1]) != 0 {
		t.Errorf("canal fechado gravou %+v", snap.messages[1])
	}
	if len(snap.messages[2]) != 1 {
		t.Errorf("canal aberto deveria ter 1 mensagem, tem %d", len(snap.messages[2]))
	}
}

func TestSnapshotsSobrepostosSaoIndependentes(t *testing.T) {
	module := moduloDeTeste(3, 0)

	module.handleUponSnapshotReq(1)
	module.handleUponSnapshotReq(2)

	// chega antes de qualquer marcador: entra nos dois
	module.recordInTransit(formatRespOK(1))

	// o marcador do snapshot 1 fecha o canal 1 so para ele
	module.handleUponDeliverMarker(formatMarker(1, 1))
	module.recordInTransit(formatRespOK(1)) // so o snapshot 2 grava

	snap1 := module.snapshots[1]
	snap2 := module.snapshots[2]
	if len(snap1.messages[1]) != 1 {
		t.Errorf("snapshot 1 gravou %d mensagens no canal 1, esperado 1", len(snap1.messages[1]))
	}
	if len(snap2.messages[1]) != 2 {
		t.Errorf("snapshot 2 gravou %d mensagens no canal 1, esperado 2", len(snap2.messages[1]))
	}
}

func TestMarcadorNaoEntraNoEstadoDoCanal(t *testing.T) {
	module := moduloDeTeste(3, 0)
	module.handleUponSnapshotReq(1)

	module.recordInTransit(formatMarker(9, 1))

	if len(module.snapshots[1].messages[1]) != 0 {
		t.Errorf("marcador foi gravado como mensagem do canal")
	}
}
