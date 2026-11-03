# libglass (Rust)

Port em Rust do [libGlass](../libglass), uma biblioteca de computação distribuída
em C++. Um `GlassServer` coordena vários `GlassClient`s, que se comunicam por
eventos, variáveis compartilhadas e barreiras.

```rust
use libglass::{Barrier, Event, GlassClient, GlassServer, Mode, EVERYBODY_ID};

let server = GlassServer::bind("127.0.0.1:9191")?;
let client = GlassClient::connect("127.0.0.1:9191", "node0")?;

let event = Event::<i32>::new(&client, "contador", Mode::Local)?;
event.enqueue(&42, EVERYBODY_ID, true)?;
let valor = event.try_get()?;

let barreira = Barrier::new(&client, 0, 0, true, Mode::Local)?;
barreira.sync()?;
```

```
cargo test
```

## Vários servidores

Peers formam um grupo: todos se conectam entre si, e os clientes de qualquer um
deles compartilham os objetos criados com `Mode::Global`. Gateways ligam grupos.

```rust
// Um grupo com dois peers. Basta conhecer um servidor do grupo.
let p1 = GlassServer::bind_peer("0.0.0.0:7001", None)?;
let p2 = GlassServer::bind_peer("0.0.0.0:8001", Some("host1:7001"))?;

// Dois grupos, ligados por seus gateways. Os peers entram pelo gateway.
let g1 = GlassServer::bind_remote_peer("0.0.0.0:7001", None)?;
let g2 = GlassServer::bind_remote_peer("0.0.0.0:7002", Some("host1:7001"))?;
let p = GlassServer::bind_peer("0.0.0.0:8002", Some("host2:7002"))?;
```

Os pacotes não dão voltas porque cada nível só repassa ao outro: o que vem de um
cliente vai para peers e gateways, o que vem de um peer vai só para outros grupos,
e o que vem de outro grupo vai só para os peers locais.

Cada servidor tem seus próprios clientes e seu próprio master. Se o `seed` não
responder, o servidor começa um grupo novo, como no C++.

## O que já foi portado

| Parte do C++ | Situação |
|---|---|
| `Packet`, formato de rede | Portado |
| `TCP` | Portado |
| `GlassServer`, `GlassClient` | Portados |
| `Event`, `Shared`, `Barrier` | Portados, inclusive entre servidores |
| `GlassServerPeer`, `GlassRemotePeer` | Portados, com lógica global refeita (veja abaixo). `lowpeer` não foi portado |
| `Alias`, `RemoteVar` (`Remote`) | Portados, com as partes que faltavam no C++ (veja abaixo). `dissociate` não foi portado |
| `Packager` | Parcial: `Wire` (texto, `Raw`, `Bytes`), sem Boost Serialization |
| `Messenger`, `Stream` | Falta |
| `SerializableObject`, `EventSerializable` | Falta |
| UDP, TCP+UDP, SCTP, Unix sockets | Falta |
| Memória compartilhada (`Interprocess`, `MemoryManager`) | Falta |
| Registro de plugins definidos pelo usuário | Falta |
| Bindings (Java, SWIG), demos, `geditor`, `gtracer` | Falta |

## Compatibilidade com o C++

O formato dos pacotes é o mesmo do C++, conferido por testes com bytes derivados
da leitura do código original. **Ainda não foi testado contra um binário C++ real.**

Entre servidores, as mensagens têm o mesmo formato, mas **um grupo não pode misturar
peers em C++ e em Rust**. A sequência de conexão mudou, e as atualizações de `Shared`
passaram a carregar no campo `from` o id do cliente de origem.

`Remote` usa por padrão o formato de texto de `Wire`, enquanto o C++ usa `rawPack`. Para
conversar com um cliente em C++, use `Raw<T>`.

## `Alias` e `Remote`: o que mudou em relação ao C++

`Alias`:

- O campo `associated` não é inicializado no C++, então `checkAlias` pode responder
  qualquer coisa antes da primeira associação.
- `associate(map)` envia um pacote vazio no C++. Aqui é `associate_many`.
- `deleteAlias` é um `TODO` no C++ e aqui funciona.
- Uma associação para `EVERYBODY_ID` é guardada no C++, mas nunca consultada. Aqui ela
  vale também para quem criar o alias depois, e substitui as associações individuais
  feitas antes dela.
- Associações de um cliente que saiu são esquecidas, porque o id pode ser reaproveitado
  por um cliente novo.
- `dissociate` é declarado no C++ sem implementação e sem mensagem no protocolo, então
  não existe aqui.
- `Alias<T>` aceita qualquer tipo clonável, inclusive closures dentro de `Arc`.

`Remote`:

- `GLASS_GET` e `GLASS_VALUE` são `TODO` no C++, e `getValue` de outro nó devolve
  referência para uma variável local já destruída. Aqui `get_value` pede o valor e
  espera a resposta. Se o nó não existir ou não tiver a variável, retorna `Timeout`.
- `copyDataToRemote` e `copyDataFromRemote` estão desativados no C++ e sempre retornam
  `false`. Aqui funcionam.
- `SET` só chega a quem é dono da variável, e uma vez só. O C++ reenvia aos peers
  depois de já ter enviado.
- Um cliente em C++ não responde a `GET`, então `get_value` para um deles expira.

## Peers: o que mudou em relação ao C++

A lógica global do C++ tem defeitos de fundo, alguns marcados no próprio código como
"não está funcionando como esperado". Por isso ela foi refeita em vez de traduzida.
O que segue vem da leitura do código, sem execução do original.

- **Barreira global:** no C++, `SYNC` nunca chega aos outros servidores, então ela
  não sincroniza entre eles. Agora sincroniza de verdade.
- **Objetos criados antes de um peer conectar:** o `TODO` do projeto original registra
  que nunca chegam a ele, e `synchronize()` é um stub. Agora ele os recebe ao conectar,
  com os valores atuais de `Shared`.
- **Relação entre nós:** `PEER_LIST` não transmite a relação, então todo nó da lista
  vira `RemotePeer`.
- **Duplicação de conexões:** cada `PEER_LIST_UPDATE` reconecta a todos os peers, e há
  uma conexão para cada sentido. Agora há uma só por par de servidores.
- **Laços de pacotes:** o reenvio de eventos entre peers não impede que um pacote
  volte a quem já o recebeu. Agora a hierarquia impede isso.
- **`PEER_CHANGE_ID`:** não é decodificado, porque `parse` cai no caso padrão e lança
  exceção, e a resposta vai para um id ainda não registrado. Agora o conflito de ids é
  resolvido na entrada.
- **`Shared` entre peers:** um id de peer podia ficar como "dono" da variável em outro
  servidor e bloquear atualizações para sempre. Agora a posse é do cliente de origem, e
  liberá-la propaga para os demais servidores, que chegam ao mesmo valor.
- **Saída de um nó:** apagar um objeto global, ou um peer cair, agora avisa os outros
  servidores e libera barreiras presas nele.

Limitações que continuam:

- Um peer que cai não é reconectado.
- Se a ligação entre dois gateways cair, a posse de uma variável `Shared` por um
  cliente do outro grupo fica presa até o fim do servidor: clientes de id maior não
  conseguem mais alterá-la.
- Dois servidores que entram por seeds diferentes ao mesmo tempo podem escolher o mesmo
  id. O conflito é detectado na conexão entre eles, mas não corrigido.

## Diferenças em relação ao C++

**API**

- Os objetos recebem o cliente explicitamente (`Event::new(&client, ...)`), em vez
  do "Glass atual" global (`GlassManager`).
- Erros são `Result`, em vez de exceções e códigos de retorno.
- `kill_all` para o servidor e desconecta os clientes, sem encerrar o processo. O
  C++ chama `exit()` dentro da biblioteca.
- Eventos ganharam `get_timeout`, que espera sem precisar de `sleep`.
- `Shared::new` e `Event::new` não têm equivalente aos métodos `factory`: criar
  de novo o mesmo nome retorna `AlreadyExists`.
- `nickname_to_id` retorna erro para apelido desconhecido. O C++ retorna
  `everybodyId`, o que mandaria a mensagem para todos.

**Arquitetura**

- Uma thread por conexão lê pacotes e uma única thread os processa no servidor,
  dona do estado dos plugins. Isso substitui `select()`, limitado a `FD_SETSIZE`
  conexões, e os mutexes recursivos.
- Usa threads da biblioteca padrão, sem runtime assíncrono.

**Bugs do C++ que não foram trazidos**

- `tcp.cpp`: os tamanhos vindos da rede não são validados. `length < headerlen` ou
  `headerlen < 6` causam underflow e leituras e escritas fora do buffer.
- `tcp.cpp`: o `recv` inicial de 6 bytes assume que nunca há leitura parcial.
- `packetclientserver.cpp`: confia no `count` recebido em `NODE_LIST`.
- O servidor confia no campo `from`, o que permite um cliente se passar por outro.
  Aqui, o remetente é determinado pela conexão.
- `event.cpp`: usa o ponteiro `ev` sem inicializar ao apagar evento global, e apaga
  itens durante a iteração em `unregisterNode`.
- `barrier.cpp`: `sync()` retorna com o mutex travado num dos caminhos, e
  `unregisterNode` invalida iteradores.
- `shared.cpp`: `getUpdate` não espera a resposta do servidor (`TODO` no original).
- `client.cpp`: `this->~GlassClient()` ao receber `BYE`.

## Comportamento mantido de propósito

- O servidor reescreve o remetente dos eventos com o próprio id, então o cliente
  não sabe quem enviou um evento.
- Em `Shared`, um cliente de id menor tem prioridade sobre os de id maior até
  chamar `get_update`. O original marca essa lógica como "não 100% correta", e ela
  merece ser redesenhada.
