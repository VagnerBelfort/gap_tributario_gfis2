"""Gera o pacote de entrega para o cliente (SEFAZ-MA) — relatório de proveniência.

Recalcula, direto dos arquivos-fonte versionados no repo, todos os números de
Importações (MDIC ComexStat) e ICMS arrecadado (SIGDEF e GFIS2) que aparecem no
relatório de proveniência, e materializa:

  output/entrega_cliente/
    dados_importacoes_icms_ma_goldenx.xlsx   Excel consolidado (6 abas)
    IMP_2019.csv .. IMP_2025.csv             MDIC ComexStat bruto (já filtrado UF=MA)
    ncm_transito.yaml                        lista de capítulos NCM excluídos

Uso:
    PYTHONPATH=src uv run python scripts/gera_entrega_cliente.py
"""

from __future__ import annotations

import shutil
from pathlib import Path

import polars as pl
import xlsxwriter

RAIZ = Path(__file__).resolve().parent.parent
DEST = RAIZ / "output" / "entrega_cliente"

# PTAX média anual (cotação de venda, dias úteis) — API Olinda/BCB,
# CotacaoDolarPeriodo, conferida em 10/06/2026. Mesma fonte do pipeline.
PTAX = {
    2019: 3.9461,
    2020: 5.1578,
    2021: 5.3956,
    2022: 5.1655,
    2023: 4.9953,
    2024: 5.3920,
    2025: 5.5859,
}

ANOS = list(range(2019, 2026))
CAPITULOS_TRANSITO = [27, 31]


def carrega_importacoes() -> pl.DataFrame:
    frames = []
    for ano in ANOS:
        df = pl.read_csv(RAIZ / "mdic_comex" / "dados" / f"IMP_{ano}.csv", separator=";")
        frames.append(df)
    df = pl.concat(frames)
    return df.with_columns((pl.col("CO_NCM") // 1_000_000).alias("CAPITULO"))


def tabela_importacoes_anual(imp: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for ano in ANOS:
        d = imp.filter(pl.col("CO_ANO") == ano)
        bruto = d["VL_FOB"].sum()
        excl = d.filter(pl.col("CAPITULO").is_in(CAPITULOS_TRANSITO))["VL_FOB"].sum()
        liq = bruto - excl
        ptax = PTAX[ano]
        rows.append({
            "ano": ano,
            "fob_bruto_usd": bruto,
            "fob_cap27_31_usd": excl,
            "fob_liquido_usd": liq,
            "pct_transito": round(excl / bruto * 100, 1),
            "ptax_media_anual": ptax,
            "bruto_rs_mi": round(bruto * ptax / 1e6, 1),
            "liquido_rs_mi": round(liq * ptax / 1e6, 1),
        })
    return pl.DataFrame(rows)


def tabela_capitulos(imp: pl.DataFrame) -> pl.DataFrame:
    return (
        imp.group_by("CO_ANO", "CAPITULO")
        .agg(pl.col("VL_FOB").sum().alias("FOB_USD"))
        .with_columns(pl.col("CAPITULO").is_in(CAPITULOS_TRANSITO).alias("EXCLUIDO_TRANSITO"))
        .sort("CO_ANO", "FOB_USD", descending=[False, True])
    )


def tabela_sigdef() -> tuple[pl.DataFrame, pl.DataFrame]:
    s = pl.read_parquet(RAIZ / "src" / "gap_tributario" / "data" / "sigdef_icms_ma.parquet")
    mensal = (
        s.filter(pl.col("ano").is_between(2019, 2023))
        .sort("ano", "mes")
        .with_columns((pl.col("icms_total") / 1e6).round(2).alias("icms_rs_mi"))
        .drop("icms_total")
    )
    anual = (
        mensal.group_by("ano").agg(pl.col("icms_rs_mi").sum().round(1)).sort("ano")
    )
    return anual, mensal


def tabela_gfis2() -> pl.DataFrame:
    lf = pl.scan_parquet(
        RAIZ / "bases" / "bases_arrecadacao_cruzamento" / "gfis2_ouro" / "g_arrecadacao" / "*.parquet"
    )
    df = (
        lf.filter(pl.col("per_aaaa").is_between(2019, 2025))
        .group_by("per_aaaa")
        .agg(
            pl.col("val_icms_normal").sum(),
            pl.col("val_icms_imp").sum(),
            pl.col("val_icms_st_sda").sum(),
        )
        .sort("per_aaaa")
        .collect()
    )
    return df.with_columns(
        (
            (pl.col("val_icms_normal") + pl.col("val_icms_imp") + pl.col("val_icms_st_sda")) / 1e6
        ).round(1).alias("icms_total_rs_mi"),
        (pl.col("val_icms_normal") / 1e6).round(1),
        (pl.col("val_icms_imp") / 1e6).round(1),
        (pl.col("val_icms_st_sda") / 1e6).round(1),
    )


LEIA_ME = [
    ["Pacote de dados — Gap Tributário ICMS-MA · proveniência de Importações e ICMS arrecadado"],
    ["Preparado por GoldenX Consultoria para a SEFAZ-MA · Projeto GFIS-2"],
    [""],
    ["Aba", "Conteúdo", "Fonte primária", "Como verificar"],
    [
        "Importacoes_Anual",
        "FOB US$ bruto, excluído (cap.27/31) e líquido por ano; PTAX; valores em R$ mi",
        "MDIC ComexStat, base bruta por NCM — https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta (arquivos ncm/IMP_{ANO}.csv)",
        "Somar VL_FOB dos IMP_{ANO}.csv anexos; excluir NCMs iniciados em 27 ou 31; multiplicar pela PTAX da coluna F",
    ],
    [
        "Importacoes_por_NCM_Capitulo",
        "FOB US$ por ano e capítulo NCM (2 primeiros dígitos), com marcação do que foi excluído como trânsito",
        "Mesma base MDIC ComexStat acima, filtro SG_UF_NCM = 'MA'",
        "Agrupar os IMP_{ANO}.csv por (ano, 2 primeiros dígitos do CO_NCM)",
    ],
    [
        "ICMS_SIGDEF_Anual",
        "ICMS total arrecadado MA por ano (2019–2023), R$ mi",
        "Export 'por setor' do SIGDEF/CONFAZ, fornecido pela equipe técnica da SEFAZ-MA em fev/2026 (arquivo 20260204_por-setor.xls)",
        "Somar a coluna de ICMS total (posição 21 do export; o rótulo 'va_icms_total' está desalinhado no arquivo original) para UF=MA",
    ],
    [
        "ICMS_SIGDEF_Mensal",
        "ICMS mensal MA 2019–2023, R$ mi",
        "Mesma fonte acima",
        "Comparar mês a mês com a arrecadação interna da SEFAZ",
    ],
    [
        "ICMS_GFIS2_Anual",
        "ICMS por ano da base interna GFIS2 (camada ouro), componentes normal + importação + ST saída, R$ mi",
        "Base GFIS2 fornecida pela SEFAZ-MA (parquet, g_arrecadacao)",
        "Reproduzir a soma val_icms_normal + val_icms_imp + val_icms_st_sda por ano na base GFIS2 interna",
    ],
    [
        "NCM_Excluidos",
        "Capítulos NCM tratados como trânsito do Porto de Itaqui (excluídos das importações)",
        "config/ncm_transito.yaml do projeto (anexo)",
        "Conferir se os capítulos 27 e 31 representam carga em trânsito (UF de destino ≠ MA) no Siscomex",
    ],
    [""],
    ["PTAX: média anual da cotação de venda (dias úteis), API Olinda/BCB —"],
    ["https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/ (CotacaoDolarPeriodo)"],
    [""],
    ["Valores monetários em R$ milhões, salvo indicação contrária. FOB em US$ correntes."],
]


def escreve_excel(imp_anual, imp_cap, sig_anual, sig_mensal, gfis2):
    path = DEST / "dados_importacoes_icms_ma_goldenx.xlsx"
    wb = xlsxwriter.Workbook(str(path))
    titulo = wb.add_format({"bold": True, "font_color": "#1a3a6b", "font_size": 12})
    header = wb.add_format({"bold": True, "bg_color": "#1a3a6b", "font_color": "white", "border": 1})
    num = wb.add_format({"num_format": "#,##0.0"})
    inteiro = wb.add_format({"num_format": "#,##0"})

    ws = wb.add_worksheet("Leia-me")
    ws.set_column(0, 0, 32)
    ws.set_column(1, 3, 60)
    for i, row in enumerate(LEIA_ME):
        fmt = titulo if i in (0, 1) else (header if i == 3 else None)
        for j, val in enumerate(row):
            ws.write(i, j, val, fmt)

    def dump(nome, df, formats):
        ws = wb.add_worksheet(nome)
        for j, col in enumerate(df.columns):
            ws.write(0, j, col, header)
            ws.set_column(j, j, max(14, len(col) + 2))
        for i, row in enumerate(df.iter_rows(), start=1):
            for j, val in enumerate(row):
                ws.write(i, j, val, formats.get(df.columns[j]))
        ws.freeze_panes(1, 0)

    dump("Importacoes_Anual", imp_anual, {
        "fob_bruto_usd": inteiro, "fob_cap27_31_usd": inteiro, "fob_liquido_usd": inteiro,
        "bruto_rs_mi": num, "liquido_rs_mi": num,
    })
    dump("Importacoes_por_NCM_Capitulo", imp_cap, {"FOB_USD": inteiro})
    dump("ICMS_SIGDEF_Anual", sig_anual, {"icms_rs_mi": num})
    dump("ICMS_SIGDEF_Mensal", sig_mensal, {"icms_rs_mi": num})
    dump("ICMS_GFIS2_Anual", gfis2, {c: num for c in gfis2.columns if c != "per_aaaa"})

    ws = wb.add_worksheet("NCM_Excluidos")
    ws.set_column(0, 0, 14)
    ws.set_column(1, 1, 70)
    ws.write(0, 0, "Capítulo NCM", header)
    ws.write(0, 1, "Descrição", header)
    ws.write(1, 0, 27)
    ws.write(1, 1, "Combustíveis minerais, óleos minerais e produtos da sua destilação (petróleo e derivados)")
    ws.write(2, 0, 31)
    ws.write(2, 1, "Adubos (fertilizantes)")
    ws.write(4, 1, "Capítulo = 2 primeiros dígitos do código NCM (8 dígitos). Exclusão aplicada SOMENTE às importações.")

    wb.close()
    return path


def main():
    DEST.mkdir(parents=True, exist_ok=True)

    imp = carrega_importacoes()
    imp_anual = tabela_importacoes_anual(imp)
    imp_cap = tabela_capitulos(imp)
    sig_anual, sig_mensal = tabela_sigdef()
    gfis2 = tabela_gfis2()

    path = escreve_excel(imp_anual, imp_cap, sig_anual, sig_mensal, gfis2)
    print(f"Excel: {path}")

    for ano in ANOS:
        shutil.copy2(RAIZ / "mdic_comex" / "dados" / f"IMP_{ano}.csv", DEST / f"IMP_{ano}.csv")
    shutil.copy2(RAIZ / "config" / "ncm_transito.yaml", DEST / "ncm_transito.yaml")
    print(f"CSVs e ncm_transito.yaml copiados para {DEST}")

    print("\n== Importações anual ==")
    print(imp_anual)
    print("\n== SIGDEF anual ==")
    print(sig_anual)
    print("\n== GFIS2 anual ==")
    print(gfis2.select("per_aaaa", "icms_total_rs_mi"))
    print("\n== Capítulos 2022 (top 8) ==")
    print(imp_cap.filter(pl.col("CO_ANO") == 2022).head(8))


if __name__ == "__main__":
    main()
