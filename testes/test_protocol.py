import pytest
from sd.common.protocolo import (
    ProtocolError,
    escape_content,
    fmt_ack,
    fmt_ack_kill,
    fmt_ack_newuser,
    fmt_error,
    fmt_msg,
    fmt_newcoordinatorip,
    fmt_newid,
    parse_command,
    parse_kill,
    parse_msg,
    parse_newuser,
    unescape_content,
)


class TestParseCommand:
    def test_comandos_validos(self):
        assert parse_command("NEWUSER_12_Ana\r\n") == "NEWUSER"
        assert parse_command("  KILL_0  \n") == "KILL"
        assert parse_command("MSG_1_2_3_oi") == "MSG"
        assert parse_command("PING") == "PING"

    def test_linha_vazia(self):
        with pytest.raises(ProtocolError) as exc:
            parse_command("   \r\n")
        assert exc.value.code == "INVALID_COMMAND"


class TestParseNewuser:
    def test_validos(self):
        assert parse_newuser("NEWUSER_12_Ana\r\n") == (12, "Ana")
        assert parse_newuser("NEWUSER_12") == (12, None)
        assert parse_newuser("NEWUSER_5_Ana_Maria") == (5, "Ana Maria")
        assert parse_newuser("NEWUSER_7_  Joao___Silva_ ") == (7, "Joao Silva")

    def test_id_invalido(self):
        # ID zero
        with pytest.raises(ProtocolError) as exc:
            parse_newuser("NEWUSER_0")
        assert exc.value.code == "INVALID_ID"

        # 19 dígitos (limite é 18)
        with pytest.raises(ProtocolError) as exc:
            parse_newuser("NEWUSER_1234567890123456789_Ana")
        assert exc.value.code == "INVALID_FORMAT"

        # Dígitos Unicode (árabe-índicos: ١٢)
        with pytest.raises(ProtocolError) as exc:
            parse_newuser("NEWUSER_١٢_Ana")
        assert exc.value.code == "INVALID_FORMAT"

    def test_formato_e_nome_invalido(self):
        with pytest.raises(ProtocolError) as exc:
            parse_newuser("NEWUSER_")
        assert exc.value.code == "INVALID_FORMAT"

        # Underscore presente, mas nome resulta vazio
        for caso in ["NEWUSER_5_", "NEWUSER_5____", "NEWUSER_5_   "]:
            with pytest.raises(ProtocolError) as exc:
                parse_newuser(caso)
            assert exc.value.code == "INVALID_NAME"

        # Nome com mais de 100 caracteres
        nome_longo = "a" * 101
        with pytest.raises(ProtocolError) as exc:
            parse_newuser(f"NEWUSER_5_{nome_longo}")
        assert exc.value.code == "INVALID_NAME"


class TestParseKill:
    def test_validos(self):
        assert parse_kill("KILL_0\n") == 0
        assert parse_kill("KILL_42") == 42

    def test_invalidos(self):
        for caso in ["KILL_x", "KILL_-1", "KILL_", "KILL_1234567890123456789", "KILL_١"]:
            with pytest.raises(ProtocolError) as exc:
                parse_kill(caso)
            assert exc.value.code == "INVALID_FORMAT"


class TestParseMsgAndEscape:
    def test_msg_valida(self):
        assert parse_msg("MSG_104_1_3_Ola\r\n") == (104, 1, 3, "Ola")
        assert parse_msg("MSG_1_2_3_a_b_c") == (1, 2, 3, "a_b_c")

    def test_msg_invalida(self):
        # Faltando conteúdo
        with pytest.raises(ProtocolError) as exc:
            parse_msg("MSG_1_2_3")
        assert exc.value.code == "INVALID_FORMAT"

        # Conteúdo vazio após o último _
        with pytest.raises(ProtocolError) as exc:
            parse_msg("MSG_1_2_3_")
        assert exc.value.code == "INVALID_CONTENT"

        # ID zero ou não-ASCII
        with pytest.raises(ProtocolError) as exc:
            parse_msg("MSG_0_1_2_oi")
        assert exc.value.code == "INVALID_ID"

        with pytest.raises(ProtocolError) as exc:
            parse_msg("MSG_1_٢_3_oi")
        assert exc.value.code == "INVALID_ID"

    def test_round_trip_escape(self):
        texto_original = "Olá, tudo bem?\nLinha 2\r\n100% testado_com_underline 🚀!"
        escapado = escape_content(texto_original)

        # Garante ausência de quebras de linha no wire format
        assert "\n" not in escapado
        assert "\r" not in escapado
        assert unescape_content(escapado) == texto_original

        # Garante round-trip completo via fmt_msg -> parse_msg
        wire_msg = fmt_msg(99, 10, 20, texto_original)
        assert parse_msg(wire_msg) == (99, 10, 20, texto_original)


class TestFormatters:
    def test_formatters(self):
        assert fmt_ack(104) == "ACK_104"
        assert fmt_error("invalid format\n") == "ERROR_INVALID_FORMAT"
        assert fmt_ack_newuser(12) == "ACK_NEWUSER_12"
        assert fmt_ack_kill(0) == "ACK_KILL_0"
        assert fmt_newid(1) == "NEWID_1"
        assert fmt_newcoordinatorip("127.0.0.1", 5001) == "NEWCOORDINATORIP_127.0.0.1_5001"