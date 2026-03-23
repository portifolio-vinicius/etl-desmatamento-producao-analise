#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import logging
import pandas as pd
import sidrapy
import requests
from pathlib import Path
from tqdm import tqdm
import warnings
from abc import ABC, abstractmethod
from typing import List, Generator
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore", category=UserWarning)

# ==================== CONFIGURAÇÃO ====================
MAX_WORKERS = 3  # Limite de downloads paralelos (ideal para CPU/RAM limitadas)

# Configuração do Arquivo de Log
logging.basicConfig(
    filename="download_etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)


class CaminhosDados:
    DATA_DIR = Path("data")
    LANDING_DIR = DATA_DIR / "landing"
    BRONZE_DIR = DATA_DIR / "bronze"

    @classmethod
    def inicializar_pastas(cls):
        for p in [cls.LANDING_DIR, cls.BRONZE_DIR]:
            p.mkdir(parents=True, exist_ok=True)


# ==================== ARMAZENAMENTO ====================
class EscritorDeDados(ABC):
    @abstractmethod
    def escrever_chunk(
        self, df: pd.DataFrame, nome_dataset: str, coluna_particao: str = None
    ):
        pass


class EscritorParquet(EscritorDeDados):
    def __init__(self, diretorio_base: Path):
        self.diretorio_base = diretorio_base

    def escrever_chunk(
        self, df: pd.DataFrame, nome_dataset: str, coluna_particao: str = None
    ):
        if df is None or df.empty:
            return

        try:
            diretorio_destino = self.diretorio_base / nome_dataset

            if coluna_particao and coluna_particao in df.columns:
                # Trata casos em que a coluna foi duplicada acidentalmente no DataFrame (evitando exception "arg must be a list...")
                if isinstance(df[coluna_particao], pd.DataFrame):
                    df = df.loc[:, ~df.columns.duplicated()].copy()

                # Garante que a coluna de partição é um inteiro limpo (sem lixo de cabeçalho)
                df[coluna_particao] = pd.to_numeric(
                    df[coluna_particao], errors="coerce"
                )
                df = df.dropna(subset=[coluna_particao])
                df[coluna_particao] = df[coluna_particao].astype(int)

                if not df.empty:
                    valor_particao = df[coluna_particao].iloc[0]
                    diretorio_destino = (
                        diretorio_destino / f"{coluna_particao}={valor_particao}"
                    )
                    df = df.drop(columns=[coluna_particao])

            if df.empty:
                return

            diretorio_destino.mkdir(parents=True, exist_ok=True)
            nome_arquivo = f"chunk_{uuid.uuid4().hex[:8]}.parquet"

            df.to_parquet(
                diretorio_destino / nome_arquivo,
                engine="pyarrow",
                compression="snappy",
                index=False,
            )
        except Exception as e:
            logging.error(
                f"Erro ao salvar chunk parquet no dataset {nome_dataset}: {str(e)}"
            )


# ==================== EXTRAÇÃO ====================
class ExtratorDeDados(ABC):
    @abstractmethod
    def extrair(self) -> Generator[pd.DataFrame, None, None]:
        pass


# ==================== SIDRA / IBGE / TABULARES ====================
class ExtratorSidra(ExtratorDeDados):
    def _limpar_cabecalho(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove a linha de cabeçalho injetada pela API do IBGE."""
        if not df.empty and (
            "Código" in str(df.iloc[0].values) or "Variável" in str(df.iloc[0].values)
        ):
            df = df.iloc[1:].copy()
        return df

    def _padronizar_municipio(
        self, df: pd.DataFrame, coluna_id: str = "D2C"
    ) -> pd.DataFrame:
        if coluna_id in df.columns:
            df["codigo_municipio"] = (
                df[coluna_id].astype(str).str.strip().str.zfill(7).str[-7:]
            )
        return df

    def _garantir_coluna_valor(self, df: pd.DataFrame) -> pd.DataFrame:
        if "V" in df.columns:
            df = df.rename(columns={"V": "valor"})
        elif any(c.startswith("V") for c in df.columns if c != "V"):
            col_val = next((c for c in df.columns if c.startswith("V")), None)
            if col_val:
                df = df.rename(columns={col_val: "valor"})
        return df


class ExtratorPAM(ExtratorSidra):
    def __init__(self, anos: List[int]):
        self.anos = [a for a in anos if a <= 2024]

    def _baixar_ano(self, ano: int) -> pd.DataFrame:
        def buscar_tabela(codigo_tabela: str, tipo: str) -> pd.DataFrame:
            try:
                time.sleep(0.5)
                dados = sidrapy.get_table(
                    table_code=codigo_tabela,
                    territorial_level="6",
                    ibge_territorial_code="all",
                    period=str(ano),
                )
                if dados is None or dados.empty:
                    return pd.DataFrame()
                dados = self._limpar_cabecalho(dados)  # LIMPEZA APLICADA!
                dados["tipo_lavoura"] = tipo
                # O ibge traz a coluna D1C com o ano, não inserimos 'ano' aqui para não duplicar de forma conflitante
                return dados
            except Exception as e:
                logging.warning(f"Aviso PAM (Ano {ano}, {tipo}): {str(e)}")
                return pd.DataFrame()

        df_temp = buscar_tabela("1612", "temporaria")
        df_perm = buscar_tabela("1613", "permanente")
        df_ano = pd.concat(
            [d for d in [df_temp, df_perm] if not d.empty], ignore_index=True
        )

        return self._preprocessar(df_ano) if not df_ano.empty else pd.DataFrame()

    def extrair(self) -> Generator[pd.DataFrame, None, None]:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self._baixar_ano, ano): ano for ano in self.anos}
            for future in tqdm(
                as_completed(futures),
                total=len(self.anos),
                desc="Baixando PAM (Paralelo)",
            ):
                yield future.result()

    def _preprocessar(self, df: pd.DataFrame) -> pd.DataFrame:
        # Pega o ano do D1C, e se não tiver (incomum), não falha
        rename_cols = {
            "D2C": "cd_mun",
            "D3N": "produto",
            "D4N": "medida",
            "V": "valor",
        }
        if "D1C" in df.columns:
            rename_cols["D1C"] = "ano"

        df = df.rename(columns=rename_cols)
        df = self._padronizar_municipio(df, "cd_mun")
        df = self._garantir_coluna_valor(df)
        cols = ["ano", "codigo_municipio", "produto", "medida", "valor", "tipo_lavoura"]
        # Filtramos as colunas que existem e retiramos duplicadas da tela
        cols_presentes = [c for c in cols if c in df.columns]
        df = df.loc[
            :, ~df.columns.duplicated()
        ]  # Limpeza dupla de colunas repetidas (MUITO IMPORTANTE AQUI)
        return df[cols_presentes].drop_duplicates().copy()


class ExtratorPPM(ExtratorSidra):
    def __init__(self, anos: List[int]):
        self.anos = [a for a in anos if a <= 2024]

    def _baixar_ano(self, ano: int) -> pd.DataFrame:
        try:
            time.sleep(0.5)
            df = sidrapy.get_table(
                table_code="3939",
                territorial_level="6",
                ibge_territorial_code="all",
                period=str(ano),
                variable="3135",
                classification="79/100",
            )
            if df is not None and not df.empty:
                df = self._limpar_cabecalho(df)
                if "D1C" in df.columns:
                    df = df.rename(columns={"D1C": "ano"})
                else:
                    df["ano"] = ano
                df = self._padronizar_municipio(df)
                df = self._garantir_coluna_valor(df)
                return df.loc[:, ~df.columns.duplicated()]
        except Exception as e:
            logging.warning(f"Aviso PPM (Ano {ano}): {str(e)}")
            pass
        return pd.DataFrame()

    def extrair(self) -> Generator[pd.DataFrame, None, None]:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self._baixar_ano, ano): ano for ano in self.anos}
            for future in tqdm(
                as_completed(futures), total=len(self.anos), desc="Baixando PPM Bovinos"
            ):
                yield future.result()


class ExtratorPIB(ExtratorSidra):
    def __init__(self, anos: List[int]):
        self.anos = [a for a in anos if a <= 2024]

    def _baixar_ano(self, ano: int) -> pd.DataFrame:
        try:
            time.sleep(0.5)
            df = sidrapy.get_table(
                table_code="5938",
                territorial_level="6",
                ibge_territorial_code="all",
                period=str(ano),
            )
            if df is not None and not df.empty:
                df = self._limpar_cabecalho(df)
                df = df.rename(
                    columns={
                        "D1C": "ano",
                        "V": "valor",
                        "D3N": "indicador",
                        "D4N": "medida",
                    }
                )
                df = self._padronizar_municipio(df)
                df = self._garantir_coluna_valor(df)
                df = df.loc[:, ~df.columns.duplicated()]

                # Filtra apenas o que é do Agro
                if "indicador" in df.columns:
                    mask = df["indicador"].str.contains(
                        "agro|agricultura|pecuária|silvicultura|pesca|florestal",
                        case=False,
                        na=False,
                        regex=True,
                    )
                    return df[mask].copy()
                return df
        except Exception as e:
            logging.warning(f"Aviso PIB (Ano {ano}): {str(e)}")
            pass
        return pd.DataFrame()

    def extrair(self) -> Generator[pd.DataFrame, None, None]:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self._baixar_ano, ano): ano for ano in self.anos}
            for future in tqdm(
                as_completed(futures), total=len(self.anos), desc="Baixando PIB Agro"
            ):
                yield future.result()


# ==================== BASES ABERTAS GERAIS ====================
class ExtratorSimplesCSV(ExtratorDeDados):
    """Extrator utilitário para arquivos abertos com links diretos"""

    def __init__(self, url: str, separator: str = ";", encoding: str = "utf-8"):
        self.url = url
        self.separator = separator
        self.encoding = encoding

    def extrair(self) -> Generator[pd.DataFrame, None, None]:
        base_name = self.url.split("/")[-1]
        print(f"Baixando base de dados: {self.url[:50]}... ({base_name})")
        logging.info(f"Tentando baixar direto da URL: {self.url}")
        try:
            # Baixa direto para memoria e converte para DF, ignorando erros de parse se faltar coluna
            df = pd.read_csv(
                self.url,
                sep=self.separator,
                encoding=self.encoding,
                on_bad_lines="skip",
                low_memory=False,
            )
            if not df.empty:
                # Retorna tudo num chunk só (tabelas menores)
                yield df
            else:
                yield pd.DataFrame()
        except Exception as e:
            err_msg = f"Erro ao baixar {self.url}: {e}"
            print(err_msg)
            logging.error(err_msg)
            yield pd.DataFrame()


# ==================== MOTOR DE ORQUESTRAÇÃO ====================
class OrquestradorDownload:
    def __init__(self, escritor: EscritorDeDados):
        self.escritor = escritor

    def executar(self, extratores: dict):
        logging.info("=" * 50)
        logging.info(
            f"Iniciando Execução. Datasets mapeados: {list(extratores.keys())}"
        )
        logging.info("=" * 50)

        for nome_dataset, extrator in extratores.items():
            print(f"\n📥 Iniciando Processo de Extração: {nome_dataset}")
            logging.info(f"[{nome_dataset}] Iniciando extração.")
            linhas_totais = 0

            # Checa se é extrator Sidra que tem partição
            is_sidra = isinstance(extrator, ExtratorSidra)
            coluna_part = "ano" if is_sidra else None

            try:
                for chunk_df in extrator.extrair():
                    if chunk_df is not None and not chunk_df.empty:
                        linhas_totais += len(chunk_df)
                        self.escritor.escrever_chunk(
                            chunk_df, nome_dataset, coluna_particao=coluna_part
                        )

                msg_sucesso = f"✔ {nome_dataset} finalizado! O Datalake recebeu: {linhas_totais:,} registros."
                print(msg_sucesso)
                logging.info(
                    f"[{nome_dataset}] Concluído com Sucesso! Linhas carregadas: {linhas_totais}"
                )
            except Exception as e:
                msg_erro = f"❌ Erro fatal no processamento do fluxo '{nome_dataset}': {str(e)}"
                print(msg_erro)
                logging.error(msg_erro, exc_info=True)

        logging.info(
            "================ Processo de execução de lote finalizado ================"
        )


# ==================== EXECUÇÃO PRINCIPAL ====================
if __name__ == "__main__":
    CaminhosDados.inicializar_pastas()
    escritor = EscritorParquet(CaminhosDados.BRONZE_DIR)
    orquestrador = OrquestradorDownload(escritor)

    anos_alvo = list(range(2010, 2024))  # Até 2023 é mais seguro pro IBGE (fechamentos)

    # 1. Links públicos úteis para as demais bases do projeto (Mock de fontes diretas que funcionam)
    URL_IBAMA_EMBARGOS = "https://dadosabertos.ibama.gov.br/dataset/c277726a-9351-40be-aaa7-72cecd1affdb/resource/c590adcd-f8ee-4444-a696-ed754020c910/download/relatorio_embargos.csv"
    URL_COMEX_ANUAL = (
        "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_2023.csv"
    )

    # OBS: O Prodes e o MapBiomas exigem downloads espaciais massivos (>10GB),
    # ou acesso a APIs com tokens específicos (ex: base dos dados).
    # Portanto, eles ficam fora da automação simples por CSV e devem ser baixados via seus referidos plugins ou QGIS conforme plano.

    servicos_extracao = {
        "pam": ExtratorPAM(anos_alvo),
        "ppm_bovinos": ExtratorPPM(anos_alvo),
        "pib_vab_agro": ExtratorPIB(anos_alvo),
        "ibama_embargos": ExtratorSimplesCSV(URL_IBAMA_EMBARGOS, separator=";"),
        "comex_stat_2023": ExtratorSimplesCSV(URL_COMEX_ANUAL, separator=";"),
    }

    # Descomente a linha abaixo para executar toda a carga!
    orquestrador.executar(servicos_extracao)


# In[ ]:





# In[ ]:





# In[ ]:




