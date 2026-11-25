import subprocess
import sys
import threading
import time
from typing import Callable

from sd.config import (
    BACKOFF_CRIACAO_SEG,
    DIRETORIO_BASE,
    DIRETORIO_LOGS,
    ESPERA_NEWUSER_SEG,
    ESPERA_REPLICACAO_SEG,
    INTERVALO_ESCALADOR_SEG,
    IP_CADASTRO,
    IP_SERVIDOR,
    LIMITE_FALHAS_CRIACAO,
    MAX_SERVIDORES,
    PORTA_CONTROLE,
    TEMPO_OCIOSO_SEG,
    TIMEOUT_CRIACAO_SEG,
    TIMEOUT_INTERNO_SEG,
    ErroProtocolo,
    LoggerMetricas,
    enviar_e_receber,
    extrair_comando,
    formatar_ack_hello,
    formatar_ack_kill,
    formatar_ack_newuser,
    formatar_ack_shutdown,
    formatar_digest,
    formatar_erro,
    formatar_newcoordinatorip,
    formatar_newid,
    interpretar_hello,
    interpretar_kill,
    interpretar_newuser,
    interpretar_shutdown,
    porta_do_servidor,
)
from sd.estado import (
    Estado,
    calcular_metricas_balanceamento,
    escolher_servidor_ocioso,
    escolher_servidor_para_usuario,
    realocar_orfaos,
    servidores_a_criar,
    servidores_desejados,
    servidores_necessarios,
)


# =====================================================================
# Comunicação com o Cadastro e Gerenciamento de Processos Filhos
# =====================================================================

def _enviar_controle_cadastro(
    msg: str,
    ack_esperado: str,
    logger: LoggerMetricas,
    metrica_ok: str,
    metrica_falha: str,
    **extras,
) -> bool:
    t0 = time.monotonic()
    ultimo_erro = ""
    for tentativa in range(1, 4):
        try:
            resposta = enviar_e_receber(IP_CADASTRO, PORTA_CONTROLE, msg, timeout=TIMEOUT_INTERNO_SEG)
            if resposta == ack_esperado:
                latencia = round(time.monotonic() - t0, 4)
                logger.registrar(metrica_ok, valor=latencia, tentativas=tentativa, **extras)
                return True
            ultimo_erro = f"resposta inesperada: {resposta!r}"
        except OSError as exc:
            ultimo_erro = str(exc)

        if tentativa < 3:
            time.sleep(0.5)

    latencia = round(time.monotonic() - t0, 4)
    logger.registrar(metrica_falha, valor=latencia, tentativas=3, erro=ultimo_erro, **extras)
    return False


def enviar_newid(id_servidor: int, logger: LoggerMetricas) -> bool:
    """Informa NEWID_<id> ao Cadastro sem levantar exceção."""
    try:
        return _enviar_controle_cadastro(
            formatar_newid(id_servidor),
            f"ACK_NEWID_{id_servidor}",
            logger,
            metrica_ok="newid_enviado",
            metrica_falha="newid_falhou",
            servidor=id_servidor,
        )
    except Exception as exc:
        logger.registrar("newid_falhou", valor=0.0, tentativas=0, erro=str(exc), servidor=id_servidor)
        return False


def enviar_newcoordinatorip(ip: str, porta: int, logger: LoggerMetricas) -> bool:
    """Informa NEWCOORDINATORIP_<ip>_<porta> ao Cadastro."""
    try:
        return _enviar_controle_cadastro(
            formatar_newcoordinatorip(ip, porta),
            f"ACK_NEWCOORDINATORIP_{ip}_{porta}",
            logger,
            metrica_ok="newcoordinatorip_enviado",
            metrica_falha="newcoordinatorip_falhou",
            ip=ip,
            porta=porta,
        )
    except Exception as exc:
        logger.registrar("newcoordinatorip_falhou", valor=0.0, tentativas=0, erro=str(exc))
        return False


def iniciar_processo_servidor(id_servidor: int, ip_coord: str, porta_coord: int) -> subprocess.Popen:
    """Inicia um novo processo de servidor com saída redirecionada para logs/."""
    DIRETORIO_LOGS.mkdir(parents=True, exist_ok=True)
    caminho_saida = DIRETORIO_LOGS / f"servidor_{id_servidor}.out"
    arquivo_saida = caminho_saida.open("a", encoding="utf-8")

    comando = [
        sys.executable,
        "-m",
        "sd.servidor",
        "--id",
        str(id_servidor),
        "--coord-ip",
        ip_coord,
        "--coord-port",
        str(porta_coord),
    ]
    try:
        return subprocess.Popen(
            comando,
            cwd=DIRETORIO_BASE,
            stdout=arquivo_saida,
            stderr=subprocess.STDOUT,
        )
    finally:
        arquivo_saida.close()


def encerrar_processo(proc: subprocess.Popen) -> None:
    """Encerra suavemente um processo filho com fallback forçado após 1 s."""
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=1.0)
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
            proc.wait(timeout=1.0)
        except OSError:
            pass
    except OSError:
        pass


# =====================================================================
# Coordenador e Escalador Elástico
# =====================================================================

class Coordenador:
    """Processa comandos do protocolo de forma serializada e coordena replicação e elasticidade."""

    def __init__(self, estado: Estado, logger: LoggerMetricas, tempo_espera: float = ESPERA_NEWUSER_SEG):
        self.estado = estado
        self.logger = logger
        self.tempo_espera = tempo_espera
        self._lock_op = threading.Lock()
        self._cond = threading.Condition(self._lock_op)
        self._aguardando = 0
        self.escalador: Escalador | None = None
        self.replicador = None
        self.evento_mudanca_estado = threading.Event()
        self._tratadores: dict[str, Callable[[str], tuple[str, bool]]] = {
            "NEWUSER": self.tratar_newuser,
            "KILL": self.tratar_kill,
            "HELLO": self.tratar_hello,
            "SHUTDOWN": self.tratar_shutdown,
        }

    def registrar_tratador(self, prefixo: str, func: Callable[[str], tuple[str, bool]]) -> None:
        self._tratadores[prefixo] = func

    def anexar_escalador(self, escalador: "Escalador") -> None:
        self.escalador = escalador

    def anexar_replicador(self, replicador) -> None:
        self.replicador = replicador

    def _emitir(self, tipo_op: str, **dados) -> int:
        if self.replicador is None:
            return 0
        return self.replicador.anexar_sob_lock(tipo_op, dados)

    def _replicar(self, seq: int, orcamento: float = ESPERA_REPLICACAO_SEG) -> None:
        if self.replicador is None or seq <= 0:
            return
        timeout = max(0.2, min(orcamento, ESPERA_REPLICACAO_SEG))
        info = self.replicador.aguardar_replicacao(seq, timeout=timeout)
        if info["sincronizados"] > 0 or info["atrasados"] > 0:
            self.logger.registrar(
                "replicacao_espera",
                valor=info["decorrido"],
                seq=seq,
                sincronizados=info["sincronizados"],
                atrasados=info["atrasados"],
            )

    def _foto_balanceamento(self) -> dict:
        cargas = self.estado.cargas()
        metricas = calcular_metricas_balanceamento(cargas)
        total_u = self.estado.total_usuarios()
        necessarios = servidores_necessarios(total_u, self.estado.capacidade)
        ativos = metricas["servidores"]
        razao = round(ativos / necessarios, 4) if (total_u > 0 and necessarios > 0) else None
        return {
            **metricas,
            "razao_servidores": razao,
            "sem_servidor": len(self.estado.usuarios_sem_servidor()),
        }

    def _registrar_balanceamento(self, evento: str, foto: dict) -> None:
        self.logger.registrar("desbalanceamento", valor=foto["desbalanceamento"], evento=evento)
        self.logger.registrar("desvio_padrao_usuarios", valor=foto["desvio_padrao"], evento=evento)
        if foto["razao_servidores"] is not None:
            self.logger.registrar("razao_servidores_ativos", valor=foto["razao_servidores"], evento=evento)
        self.logger.registrar("usuarios_sem_servidor", valor=foto["sem_servidor"], evento=evento)

    def _alocar_pendentes_sob_lock(self) -> dict[int, int]:
        orfaos = self.estado.usuarios_sem_servidor()
        if not orfaos:
            return {}
        atribuicoes, _ = realocar_orfaos(orfaos, self.estado.cargas(), self.estado.capacidade)
        if not atribuicoes:
            return {}
        rejeitados = set(self.estado.aplicar_atribuicoes(atribuicoes))
        return {uid: sid for uid, sid in atribuicoes.items() if uid not in rejeitados}

    def tratar_digest(self, _linha: str) -> tuple[str, bool]:
        """Retorna DIGEST_<topo>_<crc> de forma atômica sob _lock_op."""
        with self._lock_op:
            topo = self.replicador.log_ops.topo() if self.replicador is not None else 0
            crc = self.estado.resumo_crc()
        return formatar_digest(topo, crc), False

    def tratar_linha(self, linha: str) -> tuple[str, bool]:
        try:
            cmd = extrair_comando(linha)
            tratador = self._tratadores.get(cmd)
            if tratador is None:
                return formatar_erro("INVALID_COMMAND"), False
            return tratador(linha)
        except ConnectionResetError:
            raise
        except ErroProtocolo as exc:
            return formatar_erro(exc.codigo), False
        except Exception as exc:
            self.logger.registrar("erro_interno", valor=1, erro=str(exc), linha=linha.strip())
            return formatar_erro("INTERNAL"), False

    def tratar_newuser(self, linha: str) -> tuple[str, bool]:
        id_usuario, nome = interpretar_newuser(linha)
        self.logger.registrar("newuser_recebido", valor=1, usuario=id_usuario)

        if not self.estado.eh_coordenador:
            self.logger.registrar("newuser_rejeitado", valor=1, usuario=id_usuario, motivo="NOT_COORDINATOR")
            return formatar_erro("NOT_COORDINATOR"), False

        inicio_espera = time.monotonic()
        prazo = inicio_espera + self.tempo_espera
        contou_espera = False
        foto: dict | None = None
        destino: int | None = None
        total_u = 0
        ativos = 0
        resultado = "FULL"
        seq_op = 0

        with self._cond:
            try:
                while True:
                    if self.estado.possui_usuario(id_usuario):
                        resultado = "DUPLICATE"
                        break

                    destino = escolher_servidor_para_usuario(self.estado.cargas(), self.estado.capacidade)
                    if destino is not None:
                        resultado = self.estado.adicionar_usuario(id_usuario, nome, destino)
                        if resultado == "ADDED":
                            seq_op = self._emitir("USER_ADD", uid=id_usuario, nome=nome, sid=destino)
                            foto = self._foto_balanceamento()
                        break

                    if self.escalador is None:
                        resultado = "FULL"
                        break

                    restante = prazo - time.monotonic()
                    if restante <= 0:
                        resultado = "TIMEOUT"
                        break

                    if not contou_espera:
                        self._aguardando += 1
                        contou_espera = True
                        self.escalador.notificar()

                    self._cond.wait(timeout=restante)
            finally:
                if contou_espera:
                    self._aguardando -= 1

            if resultado in ("FULL", "TIMEOUT", "NO_SERVER"):
                total_u = self.estado.total_usuarios()
                ativos = len(self.estado.ids_servidores_ativos())

        esperou = round(time.monotonic() - inicio_espera, 4)
        if contou_espera:
            self.logger.registrar("newuser_aguardou", valor=esperou, usuario=id_usuario, resultado=resultado)

        if resultado == "ADDED":
            self.evento_mudanca_estado.set()
            if self.escalador is not None:
                self.escalador.notificar()
            orcamento_restante = max(0.2, (self.tempo_espera + ESPERA_REPLICACAO_SEG) - esperou)
            self._replicar(seq_op, orcamento=orcamento_restante)
            self.logger.registrar("newuser_adicionado", valor=1, usuario=id_usuario, servidor=destino)
            assert foto is not None
            self._registrar_balanceamento("newuser", foto)
            return formatar_ack_newuser(id_usuario), False

        if resultado == "DUPLICATE":
            self.logger.registrar("newuser_duplicado", valor=1, usuario=id_usuario)
            return formatar_ack_newuser(id_usuario), False

        self.logger.registrar(
            "newuser_rejeitado",
            valor=1,
            usuario=id_usuario,
            motivo="NO_CAPACITY",
            usuarios=total_u,
            servidores_ativos=ativos,
        )
        return formatar_erro("NO_CAPACITY"), False

    def tratar_kill(self, linha: str) -> tuple[str, bool]:
        id_servidor = interpretar_kill(linha)
        self.logger.registrar("kill_recebido", valor=1, alvo=id_servidor)

        if id_servidor == self.estado.meu_id:
            return formatar_ack_kill(id_servidor), True

        if not self.estado.eh_coordenador:
            return formatar_erro("NOT_COORDINATOR"), False

        if not self.estado.servidor_conhecido(id_servidor):
            return formatar_erro("UNKNOWN_SERVER"), False

        with self._lock_op:
            estava_iniciando = self.estado.abortar_inicializacao(id_servidor)
            seq_cancelamento = self._emitir("SRV_DEAD", id=id_servidor) if estava_iniciando else 0

        if estava_iniciando:
            self._replicar(seq_cancelamento)
            if self.escalador is not None:
                self.escalador.encerrar_processo_servidor(id_servidor)
            self.logger.registrar("servidor_cancelado", valor=1, servidor=id_servidor)
            return formatar_ack_kill(id_servidor), False

        ultimo_seq = 0
        with self._lock_op:
            t0 = time.monotonic()
            orfaos = self.estado.derrubar_servidor(id_servidor)
            if orfaos is None:
                ja_morto = True
                qtd_migrados = 0
                qtd_pendentes = 0
                duracao = 0.0
                foto = None
            else:
                ja_morto = False
                ultimo_seq = self._emitir("SRV_DEAD", id=id_servidor)
                atribuicoes, pendentes = realocar_orfaos(orfaos, self.estado.cargas(), self.estado.capacidade)
                rejeitados = set(self.estado.aplicar_atribuicoes(atribuicoes))
                mapa_aplicados = {uid: sid for uid, sid in atribuicoes.items() if uid not in rejeitados}
                conjunto_pendentes = set(pendentes) | rejeitados

                for uid in sorted(orfaos):
                    sid_final = mapa_aplicados.get(uid)
                    ultimo_seq = self._emitir("USER_MOVE", uid=uid, sid=sid_final)

                qtd_migrados = len(mapa_aplicados)
                qtd_pendentes = len(conjunto_pendentes)
                duracao = round(time.monotonic() - t0, 6)
                foto = self._foto_balanceamento()

        if ja_morto:
            self.logger.registrar("kill_duplicado", valor=1, alvo=id_servidor)
            return formatar_ack_kill(id_servidor), False

        self.evento_mudanca_estado.set()
        if self.replicador is not None:
            self.replicador.remover_seguidor(id_servidor)
        self._replicar(ultimo_seq)

        self.logger.registrar("servidor_desativado", valor=1, servidor=id_servidor, motivo="kill")
        self.logger.registrar("usuarios_migrados", valor=qtd_migrados, evento="kill", servidor=id_servidor)
        self.logger.registrar("tempo_redistribuicao", valor=duracao, servidor=id_servidor)
        self.logger.registrar("usuarios_pendentes", valor=qtd_pendentes, servidor=id_servidor)
        assert foto is not None
        self._registrar_balanceamento("kill", foto)

        if self.escalador is not None:
            self.escalador.solicitar_desligamento(id_servidor)
            self.escalador.notificar()

        return formatar_ack_kill(id_servidor), False

    def tratar_hello(self, linha: str) -> tuple[str, bool]:
        id_servidor = interpretar_hello(linha)
        if not self.estado.eh_coordenador:
            return formatar_erro("NOT_COORDINATOR"), False

        agora = time.monotonic()
        ultimo_seq = 0
        endereco: tuple[str, int] | None = None

        with self._cond:
            ts_reserva = self.estado.instante_reserva(id_servidor)
            situacao = self.estado.marcar_ativo(id_servidor)
            mapa_realocados: dict[int, int] = {}
            foto: dict | None = None
            if situacao == "ACTIVATED":
                endereco = self.estado.endereco_de(id_servidor)
                ultimo_seq = self._emitir("SRV_ACTIVE", id=id_servidor)
                mapa_realocados = self._alocar_pendentes_sob_lock()
                for uid, sid in sorted(mapa_realocados.items()):
                    ultimo_seq = self._emitir("USER_MOVE", uid=uid, sid=sid)
                foto = self._foto_balanceamento()
                self._cond.notify_all()

        if situacao == "ACTIVATED":
            self.evento_mudanca_estado.set()
            if self.replicador is not None and endereco is not None:
                self.replicador.adicionar_seguidor(id_servidor, endereco[0], endereco[1])
            decorrido = round(agora - ts_reserva, 4) if ts_reserva is not None else 0.0
            self.logger.registrar("tempo_novo_servidor", valor=decorrido, servidor=id_servidor)
            self.logger.registrar("usuarios_migrados", valor=0, evento="criacao", servidor=id_servidor)
            self.logger.registrar("usuarios_recolocados", valor=len(mapa_realocados), servidor=id_servidor)
            assert foto is not None
            self._registrar_balanceamento("criacao", foto)
            if self.escalador is not None:
                self.escalador.ao_ficar_pronto(id_servidor)
                self.escalador.notificar()
            return formatar_ack_hello(id_servidor), False

        if situacao == "ALREADY_ACTIVE":
            return formatar_ack_hello(id_servidor), False

        return formatar_erro("UNKNOWN_SERVER"), False

    def tratar_shutdown(self, linha: str) -> tuple[str, bool]:
        id_servidor = interpretar_shutdown(linha)
        if id_servidor != self.estado.meu_id:
            return formatar_erro("WRONG_SERVER"), False
        return formatar_ack_shutdown(id_servidor), True

    # ------------------------------------------------------------------
    # Métodos auxiliares usados pelo Escalador
    # ------------------------------------------------------------------

    def visao_escalonamento(self) -> dict:
        with self._lock_op:
            return {
                "usuarios": self.estado.total_usuarios(),
                "aguardando": self._aguardando,
                "pendentes": len(self.estado.usuarios_sem_servidor()),
                "capacidade": self.estado.capacidade,
                "ativos": self.estado.ids_servidores_ativos(),
                "iniciando": self.estado.ids_servidores_iniciando(),
                "ociosos": self.estado.ids_servidores_ociosos(),
            }

    def reservar_servidores(self, qtd: int) -> list[int]:
        reservados: list[int] = []
        with self._lock_op:
            for _ in range(qtd):
                try:
                    sid = self.estado.reservar_servidor(IP_SERVIDOR, porta_do_servidor)
                    porta = porta_do_servidor(sid)
                    self._emitir("SRV_RESERVED", id=sid, ip=IP_SERVIDOR, port=porta)
                    reservados.append(sid)
                except ValueError:
                    break
        return reservados

    def abortar_iniciando(self, id_servidor: int, motivo: str) -> bool:
        seq = 0
        with self._lock_op:
            abortado = self.estado.abortar_inicializacao(id_servidor)
            if abortado:
                seq = self._emitir("SRV_DEAD", id=id_servidor)
        if abortado:
            self._replicar(seq)
            self.logger.registrar("servidor_abortado", valor=1, servidor=id_servidor, motivo=motivo)
        return abortado

    def desativar_ocioso(self, id_servidor: int, tempo_ocioso: float) -> bool:
        seq = 0
        with self._lock_op:
            desativado = self.estado.desativar_se_ocioso(id_servidor)
            if desativado:
                seq = self._emitir("SRV_DEAD", id=id_servidor)
                foto = self._foto_balanceamento()
            else:
                foto = None
        if desativado:
            if self.replicador is not None:
                self.replicador.remover_seguidor(id_servidor)
            self._replicar(seq)
            self.logger.registrar("servidor_desativado", valor=1, servidor=id_servidor, motivo="ocioso")
            self.logger.registrar("tempo_desativacao_ocioso", valor=round(tempo_ocioso, 4), servidor=id_servidor)
            assert foto is not None
            self._registrar_balanceamento("desativacao", foto)
        return desativado


class Escalador:
    """Controlador reativo e periódico de elasticidade (criação e remoção de servidores)."""

    def __init__(
        self,
        coordenador: Coordenador,
        logger: LoggerMetricas,
        func_criar: Callable[[int, str, int], subprocess.Popen] = iniciar_processo_servidor,
        func_newid: Callable[[int, LoggerMetricas], bool] = enviar_newid,
        func_shutdown: Callable[[str, int, int], bool] | None = None,
        func_encerrar: Callable[[subprocess.Popen], None] = encerrar_processo,
        func_tempo: Callable[[], float] = time.monotonic,
    ):
        self.coordenador = coordenador
        self.logger = logger
        self.func_criar = func_criar
        self.func_newid = func_newid
        self.func_shutdown = func_shutdown
        self.func_encerrar = func_encerrar
        self.func_tempo = func_tempo

        self._lock = threading.Lock()
        self._processos: dict[int, subprocess.Popen] = {}
        self._ocioso_desde: dict[int, float] = {}
        self._falhas_consecutivas = 0
        self._pausado_ate = 0.0
        self._no_limite = False

        self._acordar = threading.Event()
        self._parar = threading.Event()
        self._thread: threading.Thread | None = None

    def iniciar(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._parar.clear()
        self._thread = threading.Thread(target=self._laco, name="Escalador", daemon=True)
        self._thread.start()

    def parar(self) -> None:
        self._parar.set()
        self._acordar.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def notificar(self) -> None:
        self._acordar.set()

    def ao_ficar_pronto(self, _id_servidor: int) -> None:
        with self._lock:
            self._falhas_consecutivas = 0
            self._pausado_ate = 0.0

    def encerrar_processo_servidor(self, id_servidor: int) -> None:
        with self._lock:
            proc = self._processos.pop(id_servidor, None)
        if proc is not None:
            self.func_encerrar(proc)

    def solicitar_desligamento(self, id_servidor: int) -> None:
        def _tarefa():
            endereco = self.coordenador.estado.endereco_de(id_servidor)
            ok = False
            if endereco is not None and self.func_shutdown is not None:
                ip, porta = endereco
                try:
                    ok = self.func_shutdown(ip, porta, id_servidor)
                except Exception:
                    ok = False
            if not ok:
                self.encerrar_processo_servidor(id_servidor)
            else:
                with self._lock:
                    self._processos.pop(id_servidor, None)

        threading.Thread(target=_tarefa, daemon=True).start()

    def _laco(self) -> None:
        while not self._parar.is_set():
            self._acordar.wait(timeout=INTERVALO_ESCALADOR_SEG)
            self._acordar.clear()
            if self._parar.is_set():
                break
            try:
                self.avaliar()
            except Exception as exc:
                self.logger.registrar("erro_escalador", valor=1, erro=str(exc))

    def _registrar_falha_criacao(self, agora: float) -> None:
        pausou_agora = False
        with self._lock:
            self._falhas_consecutivas += 1
            falhas = self._falhas_consecutivas
            if falhas >= LIMITE_FALHAS_CRIACAO:
                self._pausado_ate = agora + BACKOFF_CRIACAO_SEG
                pausou_agora = True

        if pausou_agora:
            self.logger.registrar("spawn_pausado", valor=BACKOFF_CRIACAO_SEG, falhas=falhas)

    def avaliar(self) -> None:
        agora = self.func_tempo()

        with self._lock:
            snapshot_procs = list(self._processos.items())
        pids_mortos: set[int] = set()
        for sid, proc in snapshot_procs:
            if proc.poll() is not None:
                pids_mortos.add(sid)

        visao = self.coordenador.visao_escalonamento()
        iniciando_set = set(visao["iniciando"])

        mortos_orfaos = pids_mortos - iniciando_set
        if mortos_orfaos:
            with self._lock:
                for sid in mortos_orfaos:
                    self._processos.pop(sid, None)

        for sid in visao["iniciando"]:
            if sid in pids_mortos:
                if self.coordenador.abortar_iniciando(sid, "processo_morreu"):
                    self.encerrar_processo_servidor(sid)
                    self.logger.registrar("spawn_falhou", valor=1, servidor=sid)
                    self._registrar_falha_criacao(agora)
                continue

            res_em = self.coordenador.estado.instante_reserva(sid)
            if res_em is not None and (agora - res_em) > TIMEOUT_CRIACAO_SEG:
                if self.coordenador.abortar_iniciando(sid, "timeout"):
                    self.encerrar_processo_servidor(sid)
                    self.logger.registrar("spawn_timeout", valor=round(agora - res_em, 4), servidor=sid)
                    self._registrar_falha_criacao(agora)

        visao = self.coordenador.visao_escalonamento()
        atual = len(visao["ativos"]) + len(visao["iniciando"])
        desejado = servidores_desejados(visao["usuarios"], visao["aguardando"], visao["capacidade"])

        with self._lock:
            pausado = agora < self._pausado_ate

        if not pausado:
            qtd = servidores_a_criar(desejado, atual, MAX_SERVIDORES)
            if qtd > 0:
                self._no_limite = False
                if visao["pendentes"] > 0:
                    motivo = "pendentes"
                elif visao["aguardando"] > 0:
                    motivo = "demanda"
                else:
                    motivo = "limite"

                porta_coord = porta_do_servidor(self.coordenador.estado.meu_id)
                novos_ids = self.coordenador.reservar_servidores(qtd)
                for sid in novos_ids:
                    try:
                        proc = self.func_criar(sid, IP_SERVIDOR, porta_coord)
                        with self._lock:
                            self._processos[sid] = proc
                        threading.Thread(
                            target=self.func_newid, args=(sid, self.logger), daemon=True
                        ).start()
                        self.logger.registrar("servidor_criado", valor=1, servidor=sid, motivo=motivo)
                    except Exception as exc:
                        self.coordenador.abortar_iniciando(sid, "erro_popen")
                        self.logger.registrar("spawn_falhou", valor=1, servidor=sid, erro=str(exc))
                        self._registrar_falha_criacao(agora)
            elif desejado > MAX_SERVIDORES and atual >= MAX_SERVIDORES:
                if not self._no_limite:
                    self._no_limite = True
                    self.logger.registrar("limite_servidores", valor=MAX_SERVIDORES)
            else:
                self._no_limite = False

        ociosos_atuais = set(visao["ociosos"])
        with self._lock:
            for sid in ociosos_atuais:
                self._ocioso_desde.setdefault(sid, agora)
            for sid in list(self._ocioso_desde.keys()):
                if sid not in ociosos_atuais:
                    del self._ocioso_desde[sid]
            copia_ociosos = dict(self._ocioso_desde)

        if desejado < atual and copia_ociosos:
            candidato = escolher_servidor_ocioso(copia_ociosos, agora, TEMPO_OCIOSO_SEG)
            if candidato is not None:
                tempo_ocioso = agora - copia_ociosos[candidato]
                if self.coordenador.desativar_ocioso(candidato, tempo_ocioso):
                    with self._lock:
                        self._ocioso_desde.pop(candidato, None)
                    self.solicitar_desligamento(candidato)