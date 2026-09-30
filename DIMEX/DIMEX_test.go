package DIMEX

import "testing"

func TestBefore(t *testing.T) {
	casos := []struct {
		nome     string
		oneId    int
		oneTs    int
		othId    int
		othTs    int
		esperado bool
	}{
		{"timestamp menor vem antes", 0, 3, 1, 5, true},
		{"timestamp maior vem depois", 0, 5, 1, 3, false},
		{"empate no timestamp, id menor vem antes", 0, 4, 1, 4, true},
		{"empate no timestamp, id maior vem depois", 2, 4, 1, 4, false},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			got := before(c.oneId, c.oneTs, c.othId, c.othTs)
			if got != c.esperado {
				t.Errorf("before(%d, %d, %d, %d) = %v, esperado %v",
					c.oneId, c.oneTs, c.othId, c.othTs, got, c.esperado)
			}
		})
	}
}

func TestFormatReqEntry(t *testing.T) {
	got := formatReqEntry(0, 7)
	esperado := "reqEntry:0:7"

	if got != esperado {
		t.Errorf("formatReqEntry(0, 7) = %q, esperado %q", got, esperado)
	}
}

func TestFormatRespOK(t *testing.T) {
	got := formatRespOK(2)
	esperado := "respOK:2"

	if got != esperado {
		t.Errorf("formatRespOK(2) = %q, esperado %q", got, esperado)
	}
}

func TestParseReqEntry(t *testing.T) {
	id, timestamp, err := parseReqEntry("reqEntry:2:11")

	if err != nil {
		t.Errorf("parseReqEntry(\"reqEntry:2:11\") retornou erro: %v", err)
	}
	if id != 2 {
		t.Errorf("id = %d, esperado 2", id)
	}
	if timestamp != 11 {
		t.Errorf("timestamp = %d, esperado 11", timestamp)
	}
}

func TestParseReqEntryMissingTimestamp(t *testing.T) {
	_, _, err := parseReqEntry("reqEntry:2")

	if err == nil {
		t.Errorf("parseReqEntry(\"reqEntry:2\") deveria retornar erro, mas retornou nil")
	}
}

func TestParseReqEntryWrongType(t *testing.T) {
	_, _, err := parseReqEntry("respOK:2:11")

	if err == nil {
		t.Errorf("parseReqEntry(\"respOK:2:11\") deveria retornar erro, mas retornou nil")
	}
}

func TestParseReqEntryInvalidNumbers(t *testing.T) {
	casos := []struct {
		nome     string
		mensagem string
	}{
		{"id nao numerico", "reqEntry:x:11"},
		{"timestamp nao numerico", "reqEntry:2:y"},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			_, _, err := parseReqEntry(c.mensagem)
			if err == nil {
				t.Errorf("parseReqEntry(%q) deveria retornar erro, mas retornou nil", c.mensagem)
			}
		})
	}
}

func TestParseRespOK(t *testing.T) {
	id, err := parseRespOK("respOK:2")

	if err != nil {
		t.Errorf("parseRespOK(\"respOK:2\") retornou erro: %v", err)
	}
	if id != 2 {
		t.Errorf("id = %d, esperado 2", id)
	}
}
