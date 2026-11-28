# Documentação de Arquitetura e Regras de Negócio — Sistema Distribuído (Fases 0 a 3)

Este documento consolida todas as decisões arquiteturais, regras de negócio, protocolos e fluxos de execução implementados até o encerramento da **Fase 3**, servindo como base de referência para as próximas etapas (roteamento de mensagens, tolerância a falhas, replicação e eleição de líder).

---

## 1. Visão Geral da Arquitetura

O sistema foi projetado de forma **modular e desacoplada** para que qualquer nó da rede possa operar como um servidor comum e, quando necessário (no início ou após uma eleição), assumir dinamicamente o papel de **Coordenador** sem exigir a troca de executável.

### Separação de Responsabilidades

| Módulo | Responsabilidade Principal | Papel no Sistema |
| :--- | :--- | :--- |
| **`config.py`** | Carregamento e validação de variáveis de ambiente (`.env` e OS). | Centraliza parâmetros de rede, limites de capacidade, timeouts e codificação. |
| **`logs.py`** | Registro estruturado de eventos em formato JSON Lines (`.jsonl`). | Garante rastreabilidade *thread-safe* por componente e ID (`servidor_<id>.jsonl`). |
| **`mensagens.py`** | Definição, validação e *parsing* (via regex) do protocolo textual. | Converte strings brutas em objetos tipados (`NewUser`, `Register`, `NewId`, `Ack`, `Error`, etc.) e sanitiza entradas. |
| **`rede.py`** | Camada de transporte TCP (cliente e servidor). | Escuta conexões concorrentes (uma *thread* por conexão) e envia mensagens com *timeout* e retentativas automáticas. |
| **`coordenador.py`** | Orquestração global do cluster (classe acoplável ao `Servidor`). | Mantém a tabela global de servidores e usuários, aplica a regra de alocação determinística e gerencia o *scale-up*. |
| **`servidor.py`** | Ponto de entrada único (`--id`, `--ip`, `--porta`). | Armazena até `MAX_USERS` usuários localmente e ativa a instância de `Coordenador` quando `id == INITIAL_SERVER_ID`. |

---

## 2. Evolução por Fases

### Fase 0: Base Comum (Configuração, Logs e Protocolo)
* **Configuração Resiliente (`config.py`):** Leitura prioritária de variáveis de ambiente do sistema operacional com *fallback* para o arquivo `.env`. Valida tipos, intervalos de portas (`1..65535`) e codificação (`ENCODING`), encerrando rapidamente (`fail-fast`) caso parâmetros obrigatórios (`SERVER_IP`, `SERVER_PORT`) estejam ausentes.
* **Observabilidade (`logs.py`):** Cada processo grava eventos atômicos em `logs/servidor_<id>.jsonl` protegidos por `threading.Lock`, seguindo o esquema padrão `{"ts", "componente", "id", "metrica", "valor", ...}`.
* **Protocolo e Sanitização (`mensagens.py`):**
  * Expressões regulares estritas (`fullmatch`) para evitar injeção de comandos ou *parsing* ambíguo.
  * Sanitização automática de nomes de usuários (`sanitize_name`), substituindo underscores (`_`) e caracteres de controle/quebras de linha por espaços simples, preservando a integridade do delimitador `_` do protocolo.
  * Suporte flexível a confirmações (`ACK_<REF>`), aceitando tanto IDs numéricos de mensagens (`ACK_104`) quanto referências textuais de controle (`ACK_NEWUSER_1`, `ACK_REGISTER_1`, `ACK_KILL_0`).

---

### Fase 1: Servidor Mínimo e Idempotência Local
* **Capacidade Limitada:** Cada instância de `Servidor` mantém um dicionário local `self.usuarios` (`user_id -> nome`) limitado a `MAX_USERS` (padrão: `5`), protegido por `threading.Lock`.
* **Regras de Idempotência e Conflito no Cadastro (`adicionar_usuario`):**
  1. **Reenvio idempotente:** Se `user_id` já existe no servidor com o **mesmo nome**, o servidor não duplica o registro, registra `newuser_duplicado` no log e retorna `ACK_NEWUSER_<user_id>`.
  2. **Conflito de identidade:** Se `user_id` já existe com **nome diferente**, o servidor rejeita a operação e retorna `ERROR_USER_EXISTS`.
  3. **Capacidade esgotada:** Se o servidor já atingiu `MAX_USERS`, retorna `ERROR_SERVER_FULL`.

---

### Fase 2: Separação de Rede, Concorrência e Registro de Nós
* **Concorrência na Camada TCP (`rede.py`):**
  * `servir(porta, tratador, stop_event)`: Escuta em `0.0.0.0:<porta>` e despacha cada socket aceito para uma *thread* dedicada (`daemon=True`), evitando que conexões persistentes bloqueiem outros clientes. Utiliza `settimeout(1.0)` no `accept()` para permitir encerramento limpo via `Ctrl+C` (`KeyboardInterrupt`) em sistemas Windows e Linux.
  * `enviar(ip, porta, linha, ...)`: Implementa comunicação síncrona linha-a-linha com *retries* configuráveis (`SEND_RETRIES`, `RETRY_DELAY`, `SEND_TIMEOUT`).
* **Identidade via CLI (`servidor.py`):**
  * Os servidores passaram a receber `--id`, `--ip` (endereço real anunciado ao cluster, evitando `0.0.0.0` em ambientes multi-máquina) e `--porta`.
  * `SERVER_IP:SERVER_PORT` no `.env` passou a representar o endereço do **Coordenador Inicial**.
* **Auto-registro (`REGISTER`):**
  * Ao iniciar com `id != INITIAL_SERVER_ID`, o servidor abre sua porta de escuta e dispara uma *thread* para enviar `REGISTER_<ID>_<IP>_<PORTA>` ao coordenador.
  * O coordenador registra o nó em `self.servidores` (`{id: (ip, porta, estado)}`) com estado inicial `"reserva"` (ou preserva `"ativo"`/`"ativando"` em caso de reenvio) e responde `ACK_REGISTER_<ID>`.

---

### Fase 3: Escalabilidade Elástica (*Scale-up*) e Distribuição Determinística

Nesta etapa, o `Coordenador` passou a atuar como orquestrador de capacidade e roteador de cadastros.

#### 1. Regra de Alocação Determinística (Menor ID Ativo com Vaga)
* **Escolha:** *First-Fit* ordenado por ID crescente (`sorted(ativos)`).
* **Justificativa:**
  * Garante **empacotamento máximo** dos servidores ativos, mantendo a proporção exata exigida de $\text{servidores ativos} \div \lceil U / \text{MAX\_USERS} \rceil = 1$.
  * **Zero migração de estado:** Ao adicionar um novo servidor ao cluster, os usuários já alocados nos servidores anteriores permanecem exatamente onde estão (diferente de *hash* modular `user_id % N`, que exigiria realocação e causaria colisões acima de 5 usuários).
  * O coordenador mantém o mapa global `self.usuarios_alocados` (`user_id -> server_id`) para rastrear a localização exata de cada usuário.

#### 2. Ciclo de Vida e Estados dos Servidores
Cada nó na tabela `self.servidores` transita entre três estados:
* **`"reserva"`:** Servidor registrado e ocioso, ainda invisível para o módulo de `Cadastro`.
* **`"ativando"`:** Estado intermediário transitório aplicado sob `Lock` durante o processo de criação/notificação ao `Cadastro`. Impede condições de corrida (*race conditions*) caso múltiplas requisições `NEWUSER` cheguem simultaneamente no limite de capacidade.
* **`"ativo"`:** Servidor confirmado junto ao `Cadastro` via `NEWID_<id>` e apto a receber usuários.

#### 3. Fluxo Completo de Cadastro de Usuário (`processar_novo_usuario`)
1. **Checagem de Idempotência Global:** Se `user_id` já consta em `self.usuarios_alocados`, o coordenador encaminha o `NEWUSER` diretamente ao servidor dono daquele usuário (sem consumir novas vagas), delegando a resposta final (`ACK` ou `ERROR_USER_EXISTS`) ao nó responsável.
2. **Alocação em Servidor Ativo:** Busca o servidor `"ativo"` de menor ID com `ocupacao < MAX_USERS` e pré-reserva a vaga (`ocupacao[sid] += 1`) dentro de um *lock* curto (sem envolver I/O de rede).
3. **Escalonamento Híbrido (Pool de Reserva + Criação Sob Demanda):**
   * Se todos os ativos estiverem cheios, o coordenador verifica primeiro se há algum servidor `"reserva"` pré-registrado (suportando execução distribuída via SSH em múltiplas máquinas).
   * Caso não haja reservas disponíveis, aciona `_criar_servidor_sob_demanda()`, que calcula o próximo ID e porta livres, instancia um novo processo `servidor.py` localmente via `subprocess.Popen` e aguarda o recebimento do seu `REGISTER` através de um `threading.Event` (com *timeout* de 5 segundos).
4. **Notificação ao Cadastro (`NEWID_<id>`):** Antes de enviar o usuário ao servidor recém-escolhido, o coordenador envia `NEWID_<id>` para `CADASTRO_IP:CONTROL_PORT` (`6001`). Apenas após confirmação positiva o nó é promovido a `"ativo"`.
5. **Entrega e Consolidação:** O `NEWUSER` é entregue localmente (se `server_id == self.servidor.id`) ou via `rede.enviar`. Confirmado o `ACK`, o vínculo `usuarios_alocados[user_id] = server_id` é consolidado.

#### 4. Garantia de Atomicidade (*Rollback*) e Encerramento Limpo
* **Falha na comunicação com o Cadastro (`NEWID`):** Se o `Cadastro` estiver inacessível, o servidor volta de `"ativando"` para `"reserva"` e o coordenador retorna `ERROR_CADASTRO_UNREACHABLE`.
* **Falha na entrega ao Servidor Destino (`NEWUSER`):** Caso o envio do usuário falhe após a reserva da vaga, `_liberar_vaga()` decrementa `ocupacao[sid]` para evitar vazamento de capacidade ("vagas fantasmas").
* **Gestão de Processos Filhos:** Ao receber `KeyboardInterrupt` (`Ctrl+C`), o coordenador executa `encerrar_filhos()`, enviando `terminate()` a todos os processos criados sob demanda para não deixar processos órfãos no sistema operacional.

---

## 3. Resumo dos Contratos de Mensagens Implementados

| Mensagem | Direção | Resposta Esperada | Finalidade |
| :--- | :--- | :--- | :--- |
| `NEWUSER_<ID>[_<NOME>]` | Cadastro $\rightarrow$ Coord. $\rightarrow$ Servidor | `ACK_NEWUSER_<ID>` / `ERROR_*` | Cadastra usuário com idempotência e *scale-up* automático. |
| `REGISTER_<ID>_<IP>_<PORT>` | Servidor $\rightarrow$ Coordenador | `ACK_REGISTER_<ID>` | Anuncia um novo servidor ao coordenador inicial. |
| `NEWID_<ID>` | Coordenador $\rightarrow$ Cadastro (`:6001`) | `ACK_NEWID_<ID>` | Informa ao Cadastro que um novo servidor foi ativado. |
| `KILL_<ID>` | Cadastro / Teste $\rightarrow$ Coordenador | `ACK_KILL_<ID>` | Reconhecido no protocolo (será expandido na fase de falhas/eleição). |

## Fase 4: Troca de Mensagens entre Usuários

### Objetivo
Implementar o fluxo contínuo de troca de mensagens entre usuários distribuídos em diferentes servidores do cluster, passando pelo roteamento centralizado do coordenador, processamento simulado no destino e confirmação ponta a ponta (`ACK`).

### Alterações Realizadas

* **`mensagens.py`:**
  * Adicionada a mensagem auxiliar `UserList` (`USERS_<id1>,<id2>,...`) e sua expressão regular (`USERS_RE`) para sincronização dos IDs de usuários ativos no cluster.
  * Adicionada a sanitização de conteúdo (`sanitize_content`) para `Msg` (`MSG_<msg_id>_<sender_id>_<dest_id>_<conteudo>`), removendo quebras de linha e caracteres de controle para preservar o enquadramento do protocolo TCP.
  * Padronizada a classe `Ack` (`ACK_<referencia>`) para suportar tanto confirmações de mensagens (`ACK_<msg_id>`) quanto comandos internos.

* **`servidor.py`:**
  * Criada uma thread *daemon* dedicada por usuário hospedado (`_loop_usuario`), disparada automaticamente em `adicionar_usuario` via `_iniciar_thread_usuario`.
  * Cada thread gera `msg_id` únicos sem colisão (`sender_id * 1_000_000 + seq`) e envia mensagens em intervalos aleatórios de até `MAX_MSG_INTERVAL` (`10 s`) para destinatários válidos da lista `usuarios_conhecidos`.
  * Implementados `receive_msg` (com atraso simulado de `RECEIVE_MSG_DELAY = 1.0 s`) e `receber_mensagem`, garantindo idempotência no destino por meio do conjunto `mensagens_recebidas` protegido por `Lock`.
  * Adicionado suporte ao recebimento de `UserList` (`USERS_...`) para atualização dinâmica da lista local de destinatários.

* **`coordenador.py`:**
  * Implementado o método `rotear_mensagem`, que consulta `usuarios_alocados` para localizar o servidor de destino, realiza entrega local (quando o destinatário está no próprio coordenador) ou encaminha via TCP (`rede.enviar` com `ACK_TIMEOUT` e `MAX_ACK_RETRIES`), devolvendo o `ACK_<msg_id>` ao remetente.
  * Implementado `_difundir_lista_usuarios`, invocado após cada novo cadastro confirmado em `processar_novo_usuario`, mantendo todos os servidores ativos sincronizados quanto aos destinatários disponíveis.

### Validação
* Testes realizados com usuários distribuídos entre múltiplos servidores (`servidor_0` e `servidor_1`).
* Os arquivos de log (`logs/servidor_*.jsonl`) confirmaram o ciclo completo e correlacionado pelo `msg_id`: `msg_enviada` $\rightarrow$ `msg_roteada` $\rightarrow$ `msg_recebida` $\rightarrow$ `ack_enviado` $\rightarrow$ `ack_recebido`.