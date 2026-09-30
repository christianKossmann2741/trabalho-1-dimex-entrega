/*  Snapshot distribuido de Chandy-Lamport sobre o DiMEx.

    O marcador viaja pelo mesmo canal das mensagens do protocolo, entao a ordem
    FIFO do PP2PLink garante que tudo que foi enviado antes do marcador chega
    antes dele. Cada processo congela seu estado local na primeira vez que ve o
    snapshot (por pedido da aplicacao ou pelo primeiro marcador recebido) e a
    partir dai grava as mensagens que chegam em cada canal ate o marcador
    daquele canal aparecer.
*/

package DIMEX

import (
	"fmt"
	"strconv"
	"strings"
)

// ------------------------------------------------------------------------------------
// ------- tipos que o avaliador offline consome
// ------------------------------------------------------------------------------------

// LocalState e a foto do estado interno de um processo no instante do corte.
type LocalState struct {
	ProcessID int    `json:"processID"`
	State     string `json:"state"`
	Waiting   []bool `json:"waiting"`
	Lcl       int    `json:"lcl"`
	ReqTs     int    `json:"reqTs"`
	NbrResps  int    `json:"nbrResps"`
}

// ChannelMessage e uma mensagem do protocolo que estava em transito no corte.
type ChannelMessage struct {
	From    int    `json:"from"`
	Content string `json:"content"`
}

// SnapshotRecord e o que cada processo grava em disco para um snapshot.
type SnapshotRecord struct {
	SnapshotID int                      `json:"snapshotID"`
	ProcessID  int                      `json:"processID"`
	Local      LocalState               `json:"local"`
	Channels   map[int][]ChannelMessage `json:"channels"`
	Fault      string                   `json:"fault"`
}

// StateName devolve o nome textual de um estado do DiMEx.
func StateName(st State) string {
	switch st {
	case noMX:
		return "noMX"
	case wantMX:
		return "wantMX"
	case inMX:
		return "inMX"
	}
	return "desconhecido"
}

// ------------------------------------------------------------------------------------
// ------- protocolo do marcador
// ------------------------------------------------------------------------------------

func formatMarker(snapshotID, senderID int) string {
	return fmt.Sprintf("marker:%d:%d", snapshotID, senderID)
}

func parseMarker(message string) (snapshotID int, senderID int, err error) {
	partes := strings.Split(message, ":")
	if len(partes) != 3 {
		return 0, 0, fmt.Errorf("mensagem marker com formato invalido: %q", message)
	}
	if partes[0] != "marker" {
		return 0, 0, fmt.Errorf("tipo de mensagem inesperado, esperava marker: %q", message)
	}

	snapshotID, err = strconv.Atoi(partes[1])
	if err != nil {
		return 0, 0, err
	}

	senderID, err = strconv.Atoi(partes[2])
	if err != nil {
		return 0, 0, err
	}

	return snapshotID, senderID, nil
}

// messageKind classifica a mensagem pelo prefixo exato. Uma string que apenas
// contem "respOK" no meio nao e uma resposta.
func messageKind(message string) string {
	switch {
	case strings.HasPrefix(message, "marker:"):
		return "marker"
	case strings.HasPrefix(message, "respOK:"):
		return "respOK"
	case strings.HasPrefix(message, "reqEntry:"):
		return "reqEntry"
	}
	return ""
}

// senderOf devolve o id logico de quem mandou uma mensagem do DiMEx.
// O id precisa vir dentro da mensagem porque o From do PP2PLink traz a porta
// efemera do TCP, que nao identifica o processo.
func senderOf(message string) (int, bool) {
	if strings.HasPrefix(message, "reqEntry:") {
		id, _, err := parseReqEntry(message)
		return id, err == nil
	}
	if strings.HasPrefix(message, "respOK:") {
		id, err := parseRespOK(message)
		return id, err == nil
	}
	return 0, false
}

// ------------------------------------------------------------------------------------
// ------- snapshot em andamento
// ------------------------------------------------------------------------------------

type activeSnapshot struct {
	id        int
	local     LocalState
	recording []bool // por processo: ainda estamos gravando aquele canal de entrada?
	messages  map[int][]ChannelMessage
	done      bool
}

// captureLocalState copia o estado do modulo. A copia de waiting e importante:
// sem ela o registro continuaria apontando para o slice vivo do modulo.
func (module *DIMEX_Module) captureLocalState() LocalState {
	waiting := make([]bool, len(module.waiting))
	copy(waiting, module.waiting)

	return LocalState{
		ProcessID: module.id,
		State:     StateName(module.st),
		Waiting:   waiting,
		Lcl:       module.lcl,
		ReqTs:     module.reqTs,
		NbrResps:  module.nbrResps,
	}
}

// newSnapshot congela o estado local e abre a gravacao de todos os canais de entrada.
func (module *DIMEX_Module) newSnapshot(snapshotID int) *activeSnapshot {
	recording := make([]bool, len(module.addresses))
	for i := range recording {
		recording[i] = i != module.id
	}

	return &activeSnapshot{
		id:        snapshotID,
		local:     module.captureLocalState(),
		recording: recording,
		messages:  make(map[int][]ChannelMessage),
	}
}

func (module *DIMEX_Module) broadcastMarker(snapshotID int) {
	msg := formatMarker(snapshotID, module.id)
	for i, addr := range module.addresses {
		if i == module.id {
			continue
		}
		module.sendToLink(addr, msg, "")
	}
}

// finishIfComplete entrega o registro quando todos os canais de entrada fecharam.
func (module *DIMEX_Module) finishIfComplete(snap *activeSnapshot) {
	if snap.done {
		return
	}
	for i, gravando := range snap.recording {
		if i != module.id && gravando {
			return
		}
	}

	snap.done = true
	delete(module.snapshots, snap.id)
	module.snapshotsDone[snap.id] = true

	module.SnapshotInd <- SnapshotRecord{
		SnapshotID: snap.id,
		ProcessID:  module.id,
		Local:      snap.local,
		Channels:   snap.messages,
		Fault:      string(module.fault),
	}
}

// ------------------------------------------------------------------------------------
// ------- entradas do algoritmo
// ------------------------------------------------------------------------------------

// handleUponSnapshotReq atende o pedido da aplicacao para iniciar um snapshot.
func (module *DIMEX_Module) handleUponSnapshotReq(snapshotID int) {
	if snapshotID <= 0 || module.snapshotsDone[snapshotID] {
		return
	}
	if _, jaExiste := module.snapshots[snapshotID]; jaExiste {
		return
	}

	snap := module.newSnapshot(snapshotID)
	module.snapshots[snapshotID] = snap

	module.broadcastMarker(snapshotID)
	module.finishIfComplete(snap) // sistema com um processo so ja termina aqui
}

// handleUponDeliverMarker trata um marcador vindo de outro processo.
func (module *DIMEX_Module) handleUponDeliverMarker(message string) {
	snapshotID, senderID, err := parseMarker(message)
	if err != nil {
		return
	}
	if senderID < 0 || senderID >= len(module.addresses) || senderID == module.id {
		return
	}
	if module.snapshotsDone[snapshotID] {
		return
	}

	snap, jaExiste := module.snapshots[snapshotID]
	if !jaExiste {
		// primeiro marcador deste snapshot: congela o estado e repassa adiante
		snap = module.newSnapshot(snapshotID)
		module.snapshots[snapshotID] = snap
		module.broadcastMarker(snapshotID)
	}

	snap.recording[senderID] = false
	module.finishIfComplete(snap)
}

// recordInTransit anexa a mensagem a todo snapshot que ainda grava aquele canal.
func (module *DIMEX_Module) recordInTransit(message string) {
	remetente, ok := senderOf(message)
	if !ok || remetente < 0 || remetente >= len(module.addresses) {
		return
	}

	for _, snap := range module.snapshots {
		if snap.recording[remetente] {
			snap.messages[remetente] = append(snap.messages[remetente],
				ChannelMessage{From: remetente, Content: message})
		}
	}
}
