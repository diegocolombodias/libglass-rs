# libglass

Biblioteca de computação distribuída em Rust. Um `GlassServer` coordena vários
`GlassClient`s, que se comunicam por eventos, variáveis compartilhadas, barreiras e
outros objetos. Substitui o libGlass em C++, que não é mais desenvolvido.

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

## Arquitetura

![Arquitetura: cada nó tem um GlassClient, que troca pacotes com um GlassServer. No servidor, threads de leitura alimentam uma fila única, e uma thread dispatcher entrega os pacotes aos plugins. Servidores se ligam a peers e gateways.](docs/arquitetura.svg)

Cada objeto do cliente conversa com o plugin de mesmo nome no servidor. Toda a lógica do
servidor roda em uma única thread, que processa um pacote de cada vez, então os plugins não
precisam de locks.

## Objetos

| Tipo | Para que serve |
|---|---|
| `Event<T>` | Fila de mensagens entre clientes, para um nó ou para um grupo |
| `Messenger<T>` | Mensagens ponto a ponto, em que quem recebe sabe quem enviou |
| `Shared<T>` | Variável com um valor comum, publicado e buscado explicitamente |
| `Remote<T>` | Variável própria de cada cliente, que os outros leem e alteram pelo id |
| `Barrier` | Ponto de sincronização, que pode ser desativado e reativado |
| `Alias<T>` | Valor local escolhido pelo nome que o grupo deu a este nó |
| `Stream` | Transferência de arquivos e outros dados grandes |
| `EventSerializable` | Evento que carrega objetos de classes diferentes na mesma fila |
| `SharedRegion` | Memória compartilhada entre processos da mesma máquina, sem rede |

Valores trafegam pelo trait `Wire`. Números usam texto, `Raw<T>` usa os bytes nativos e
`Bytes` envia binário sem alterar. Com a feature `serde`, `Serde<T>` envia qualquer tipo
que implemente os traits do serde.

## Vários servidores

Peers formam um grupo: todos se conectam entre si, e os clientes de qualquer um deles
compartilham os objetos criados com `Mode::Global`. Gateways ligam grupos.

```rust
// Um grupo com dois peers. Basta conhecer um servidor do grupo.
let p1 = GlassServer::bind_peer("0.0.0.0:7001", None)?;
let p2 = GlassServer::bind_peer("0.0.0.0:8001", Some("host1:7001"))?;

// Dois grupos, ligados por seus gateways. Os peers entram pelo gateway.
let g1 = GlassServer::bind_remote_peer("0.0.0.0:7001", None)?;
let g2 = GlassServer::bind_remote_peer("0.0.0.0:7002", Some("host1:7001"))?;
let p = GlassServer::bind_peer("0.0.0.0:8002", Some("host2:7002"))?;
```

Os pacotes não dão voltas porque cada nível só repassa ao outro: o que vem de um cliente
vai para peers e gateways, o que vem de um peer vai só para outros grupos, e o que vem
de outro grupo vai só para os peers locais.

Cada servidor tem seus próprios clientes e seu próprio master. Um servidor que entra
depois recebe os objetos globais já existentes. Se o `seed` não responder, o servidor
começa um grupo novo.

## Transportes

- **TCP:** o padrão.
- **TCP + UDP:** com `bind_with_udp` e `connect_with_udp`, `Event::enqueue_unreliable` usa
  UDP, que é mais rápido para dados logo substituídos por outros mais novos, mas pode
  perder pacotes. O servidor informa a porta UDP ao cliente. Se UDP não funcionar, ou o
  dado não couber em um datagrama, tudo segue por TCP.
- **SCTP:** `bind_sctp`, `bind_peer_sctp`, `bind_remote_peer_sctp` e `connect_sctp`, fora do
  Windows. Veja abaixo.
- **Unix sockets:** `bind_unix` e `connect_unix`, fora do Windows.

### SCTP

```rust
let p1 = GlassServer::bind_peer_sctp("0.0.0.0:7001", None)?;
let p2 = GlassServer::bind_peer_sctp("0.0.0.0:8001", Some("host1:7001"))?;
let client = GlassClient::connect_sctp("host1:7001", "node0")?;
```

Um servidor SCTP usa esse protocolo com os outros servidores e com os próprios clientes, e
todos os servidores de um grupo precisam usá-lo. Não é preciso instalar a `libsctp`, mas o
kernel precisa ter o módulo `sctp`, ou essas funções retornam erro. Elas não existem no Windows,
que não tem SCTP nativo.

- **Por que usar com peers:** com o servidor em `0.0.0.0`, o kernel pode usar várias interfaces
  de rede na mesma associação e trocar de caminho se uma cair. Esse failover não foi testado
  aqui.
- **O que não é usado:** os vários streams do SCTP. Tudo vai em um único stream ordenado, porque
  os objetos dependem da ordem dos pacotes.
- **Desempenho:** não espere mais velocidade que o TCP. Em Linux 6.6, com sockets puros, o TCP
  passou de 2 GB/s no loopback, e o SCTP ficou em cerca de 1,2 GB/s no loopback e 370 MB/s entre
  contêineres, com travadas ocasionais de alguns segundos no loopback em transferências grandes.
- **Fora do SCTP:** UDP, `connect_retry`, a API em C e o `render_wall` continuam só com TCP.

## Estender

Plugins próprios implementam `ServerPlugin` no servidor (`GlassServer::add_plugin`, com
ids acima de `MAX_RESERVED_PLUGIN_ID`). No cliente, usam `send_to_plugin` e
`set_plugin_handler`.

## Outras linguagens

[include/libglass.h](include/libglass.h) define a API em C. A biblioteca compilada por
`cargo build` já a exporta, e [bindings/python](bindings/python) a usa pelo `ctypes`:

```python
import libglass

with libglass.Server() as server, libglass.Client(server.address, "a") as client:
    with libglass.Event(client, "ev") as event:
        event.enqueue(b"oi", send_to_self=True)
        print(event.get())
```

## Testes e exemplo

```
cargo test --features serde
python bindings/python/test_libglass.py       # depois de cargo build
cargo run --example quicksort -- 100000 4
```

Os testes de Unix sockets não rodam no Windows, e os de SCTP são pulados onde ele não existe.
Para rodar tudo em Linux, em um contêiner descartável:

```
docker run --rm -v "${PWD}:/src:ro" rust:1 sh /src/scripts/test-linux.sh
```

**No Windows**, `cargo build` compila a biblioteca em qualquer toolchain, mas `cargo test` e
os exemplos também compilam as dependências de janela do `render_wall`, que precisam de um
destes:

- Toolchain MSVC, o padrão do Rust, com o componente C++ do Visual Studio.
- Toolchain GNU com o MinGW completo no `PATH`, porque o `dlltool` que vem com o Rust depende
  do `as`.

## Renderização distribuída

O exemplo [render_wall](examples/render_wall) é uma parede de tiles com OpenGL 3.3: cada
janela mostra um pedaço de uma única cena animada, e todas trocam de quadro juntas.

```
cargo run --example render_wall -- demo 4
```

Em várias máquinas, com um servidor e quantos nós quiser:

```
maquina1$ cargo run --example render_wall -- server 0.0.0.0:9191 --wait-for 2
maquina2$ cargo run --example render_wall -- node maquina1:9191
maquina3$ cargo run --example render_wall -- node maquina1:9191
```

| Comando, em qualquer janela | Efeito, em todas |
|---|---|
| Setas, WASD ou arrastar com o mouse | Translação |
| `+`, `-` ou roda do mouse | Escala |
| Espaço | Pausa ou retoma a animação |
| `T` | Mostra ou esconde as bordas dos tiles |
| `R` | Volta à vista inicial |
| Esc | Fecha a parede inteira |

Para ver a sincronização: a barra amarela e a barra azul atravessam os tiles, e o quadro
atual aparece em binário na parte de baixo. Se os nós mostrassem quadros diferentes, tudo
isso quebraria onde dois tiles se encontram.

**Como funciona.** Um coordenador sem janela, que roda junto do servidor, decide cada
quadro. O `Event` leva o estado do quadro a todos, o `Messenger` leva os comandos de
teclado e mouse ao coordenador, e o `Barrier` segura a troca de buffer até todos terminarem
de renderizar. Como só o coordenador calcula o estado, os nós não têm como divergir.
Veja [sync.rs](examples/render_wall/sync.rs).

- Um nó pode entrar ou cair a qualquer momento, e a tela é redividida entre os restantes.
- O nó mais lento dita a velocidade de todos. Para simular isso, use `--delay-ms`.
- Um nó travado, mas ainda conectado, para a parede inteira, porque `Barrier::sync` não tem
  timeout.
- Para testar com carga, use `--cubes`. As demais opções aparecem ao rodar sem argumentos.

**Teste de ponta a ponta.** Abre janelas de verdade e verifica se os tiles juntos formam a
mesma imagem de uma janela única, se os nós trocam de quadro juntos, e se a parede sobrevive
a um nó lento, a um nó atrasado e a um nó morto:

```
cargo build --example render_wall
python tests/render_wall/test_render_wall.py
```

As imagens e os logs de cada rodada ficam em `tests/render_wall/output`, fora do Git. Com
`xdotool` instalado, em X11, o teste também aperta teclas e usa o mouse de verdade. Para rodar
tudo isso em Linux, em um contêiner descartável com tela virtual:

```
docker run --rm -v "${PWD}:/src:ro" -v "${PWD}/tests/render_wall/output:/out" rust:1 sh /src/tests/render_wall/run-linux.sh
```

## Limitações

- Um peer que cai não é reconectado, e um cliente também não se reconecta sozinho. O que
  existe é `connect_retry`, para quando o servidor ainda não subiu.
- Se a ligação entre dois gateways cair, a posse de uma variável `Shared` por um cliente
  do outro grupo fica presa até o fim do servidor, e clientes de id maior não conseguem
  mais alterá-la.
- Dois servidores que entram por seeds diferentes ao mesmo tempo podem escolher o mesmo
  id. O conflito é detectado na conexão entre eles, mas não corrigido.
- `Stream` não tem controle de fluxo: quem recebe guarda os dados em memória até chamar
  `receive`, e quem envia pode ficar muito à frente.
- Um cliente não sabe quem enviou um `Event`, porque o servidor grava o próprio id. Para
  saber o remetente, use `Messenger`.
- Em `Shared`, um cliente de id menor tem prioridade sobre os de id maior até chamar
  `get_update`. O resultado é o mesmo em todos os servidores, mas o critério merece ser
  redesenhado.

## O que não veio do C++

| Parte | Motivo |
|---|---|
| UDP puro | No C++ não está no build e não compila. Sem entrega garantida, os outros objetos também não funcionariam nele |
| Demos com OpenGL | Dependem de GLUT e não foram portados um a um. O `render_wall` faz a divisão em tiles de `tiledrendering`, mas as vistas de CAVE e o estéreo de `cubes` não vieram |
| Bindings Java e SWIG | Dependem da API em C++. A API em C é a base para refazê-los, e não há JDK nos ambientes de teste usados |
| `geditor`, `gtracer` | Ferramentas em Java que geram código para a API em C++ e leem logs dela |
| `dissociate`, `lowpeer`, `sendUpdateAll` | No C++ não têm implementação ou uso, e o novo roteamento dispensa `lowpeer` |
| Formato do Boost Serialization | `Serde<T>` faz esse papel, em outro formato |

O formato de rede segue o do C++ em boa parte, mas a compatibilidade com ele não é um
objetivo e nunca foi testada contra um binário real.

## O que mudou em relação ao C++

**API e arquitetura**

- Os objetos recebem o cliente explicitamente, em vez do "Glass atual" global.
- Erros são `Result`, em vez de exceções e códigos de retorno.
- `kill_all` para os servidores e desconecta os clientes, sem encerrar o processo.
- Uma thread por conexão lê pacotes e uma única thread os processa no servidor, dona do
  estado dos plugins. Isso substitui o `select()`, limitado a `FD_SETSIZE` conexões, e os
  mutexes recursivos.

**O que não funcionava e agora funciona**

Vem da leitura do código, sem execução do original.

- Barreira global, que no C++ não sincronizava entre servidores.
- Objetos criados antes de um peer conectar, que nunca chegavam a ele.
- Resolução de conflito de ids entre peers.
- `Remote::get_value` e as cópias entre nós, que eram `TODO` ou estavam desativadas.
- `Messenger` e `Stream`, que no C++ estão fora do build, e `EventSerializable`, cuja
  leitura pulava o primeiro caractere do nome.
- `Barrier::disable` e `enable`, `Alias::delete_alias` e associações para todos.
- `Shared::get_update`, que agora espera a resposta do servidor.

**Erros de segurança que não vieram**

- Tamanhos vindos da rede não eram validados em `tcp.cpp`, o que permitia leituras e
  escritas fora do buffer.
- Qualquer cliente podia se passar por outro pelo campo `from`. Aqui, o remetente é
  determinado pela conexão, e UDP exige um token enviado antes por TCP.

## Licença

LGPL 2 ou posterior, como o libGlass original, de Bruno Barberi Gnecco e Corollarium
Technologies. Veja [COPYING](COPYING).
