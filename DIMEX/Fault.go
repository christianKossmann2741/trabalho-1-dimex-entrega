/*  Modos de falha injetados de proposito no DiMEx.

    Servem para mostrar que o avaliador de snapshots detecta problemas reais.
    O padrao e sempre FaultNone: a falha so entra se a aplicacao pedir.
*/

package DIMEX

import "fmt"

type FaultMode string

const (
	// FaultNone roda o algoritmo correto.
	FaultNone FaultMode = "none"

	// FaultMutex responde na hora a um pedido remoto mesmo quando o processo
	// esta em wantMX e tem prioridade. Dois processos podem entrar na SC juntos.
	FaultMutex FaultMode = "mutex"

	// FaultDeadlock adia sempre o pedido remoto quando esta em wantMX. Pedidos
	// simultaneos formam espera circular.
	FaultDeadlock FaultMode = "deadlock"
)

func ParseFaultMode(s string) (FaultMode, error) {
	switch FaultMode(s) {
	case FaultNone, FaultMutex, FaultDeadlock:
		return FaultMode(s), nil
	}
	return FaultNone, fmt.Errorf("modo de falha desconhecido: %q (use none, mutex ou deadlock)", s)
}
