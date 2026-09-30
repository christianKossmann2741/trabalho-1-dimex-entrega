# Trabalho 1: DiMEx e snapshot distribuído

Disciplina de Sistemas Distribuídos / FPPD - PUCRS.

Autores:

- Christian Kossmann Goulart
- Lavínia Garcia Winter

## Descrição

O projeto implementa exclusão mútua distribuída e snapshots de Chandy-Lamport. Três processos disputam o mesmo arquivo enquanto um deles inicia snapshots sucessivos.

O código está dividido em:

- `PP2PLink`: comunicação TCP fornecida no template da disciplina;
- `DIMEX`: exclusão mútua e snapshot;
- `SNAPSHOT`: leitura dos arquivos e verificação das invariantes;
- `cmd/dimex-snapshot`: aplicação usada nos testes;
- `cmd/snapshot-check`: avaliador dos snapshots.

## Protocolo

O DiMEx usa três mensagens:

```text
reqEntry:<id>:<timestamp>
respOK:<id>
marker:<snapshotID>:<senderID>
```

O remetente faz parte da mensagem porque o endereço recebido pelo `PP2PLink` usa uma porta TCP temporária.

Cada processo pode estar em um destes estados:

- `noMX`: não quer a seção crítica;
- `wantMX`: pediu acesso e aguarda respostas;
- `inMX`: está na seção crítica.

Pedidos concorrentes são ordenados por `(timestamp, id)`. O menor timestamp tem prioridade. Em caso de empate, vence o menor ID.

## Snapshot

O processo 0 inicia snapshots com IDs sequenciais. No primeiro contato com um ID, cada processo:

1. copia seu estado local;
2. começa a gravar os canais de entrada;
3. envia markers aos outros processos;
4. fecha cada canal quando recebe o marker correspondente.

Mensagens `reqEntry` e `respOK` recebidas antes do marker daquele canal são registradas como mensagens em trânsito. Markers não fazem parte do estado do canal.

Snapshots diferentes podem ficar ativos ao mesmo tempo. Cada um mantém sua própria cópia do estado, dos canais abertos e das mensagens registradas.

## Requisitos

- Go 1.18 ou mais recente;
- três portas TCP locais livres.

## Testes automatizados

```bash
go test -count=1 ./DIMEX ./PP2PLink ./SNAPSHOT ./cmd/...
go vet ./DIMEX ./PP2PLink ./SNAPSHOT ./cmd/...
```

## Execução com três processos

Compile a aplicação antes de iniciar os processos:

```bash
go build -o /tmp/dimex-snapshot ./cmd/dimex-snapshot
rm -rf /tmp/dimex-snapshots/correct
```

Abra três terminais e execute os comandos abaixo em sequência rápida. O processo 0 inicia 300 snapshots.

Processo 0:

```bash
/tmp/dimex-snapshot --id 0 --initiator --snapshots 300 --interval 30ms \
  --fault none --output /tmp/dimex-snapshots/correct --duration 20s \
  127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002
```

Processo 1:

```bash
/tmp/dimex-snapshot --id 1 --fault none \
  --output /tmp/dimex-snapshots/correct --duration 20s \
  127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002
```

Processo 2:

```bash
/tmp/dimex-snapshot --id 2 --fault none \
  --output /tmp/dimex-snapshots/correct --duration 20s \
  127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002
```

Os endereços devem aparecer depois das flags e na ordem dos IDs.

## Arquivos gerados

O diretório informado em `--output` recebe:

```text
process-0.jsonl
process-1.jsonl
process-2.jsonl
mxOUT.txt
```

Cada linha de um arquivo JSONL contém o estado local do processo e o estado dos canais para um snapshot.

No modo correto, `mxOUT.txt` deve conter somente repetições de:

```text
|.
```

Uma ocorrência de `||` ou `..` indica violação da exclusão mútua.

## Avaliação

```bash
go run ./cmd/snapshot-check \
  --input /tmp/dimex-snapshots/correct \
  --processes 3
```

O avaliador agrupa os registros pelo ID do snapshot e verifica:

1. no máximo um processo em `inMX`;
2. se todos estão em `noMX`, não existem respostas adiadas ou mensagens do DiMEx em trânsito;
3. um processo só adia respostas quando está em `wantMX` ou `inMX`;
4. respostas recebidas, mensagens em trânsito e respostas adiadas somam `N-1` para cada processo em `wantMX`;
5. o grafo de espera não possui ciclos;
6. os valores locais estão dentro das faixas esperadas.

O comando retorna código zero quando não encontra violações.

## Modos de falha

O argumento `--fault` aceita:

- `none`: algoritmo correto;
- `mutex`: responde a pedidos que deveriam ser adiados;
- `deadlock`: adia pedidos que deveriam receber resposta.

Para testar uma falha, use o mesmo valor nos três processos e um diretório de saída separado.

No modo `mutex`, o avaliador deve encontrar mais de um processo em `inMX`. O arquivo `mxOUT.txt` também pode conter `||` e `..`.

No modo `deadlock`, o avaliador deve encontrar um ciclo de espera e o número de acessos à seção crítica cai.

## Resultados

Foram executados testes com três processos e 300 snapshots por processo.

- `none`: 300 snapshots completos, nenhuma violação e `mxOUT.txt` válido;
- `mutex`: violações de exclusão mútua detectadas;
- `deadlock`: ciclos de espera detectados;
- snapshots sobrepostos: 300 snapshots completos com intervalo de 1 ms no modo correto.

Os números de acessos e de violações podem mudar entre execuções por causa do escalonamento dos processos.

## Programas do template

Os arquivos abaixo vieram na raiz do template e possuem funções `main` separadas:

```text
chatComPPLink.go
useDIMEX.go
useDIMEX-f.go
```

Por isso, `go build ./...` falha ao tentar compilá-los como um único programa. Execute cada arquivo separadamente ou use os comandos de teste e build mostrados acima.