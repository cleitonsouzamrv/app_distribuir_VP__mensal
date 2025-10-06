# app.py
# Streamlit — processamento em lote (até 100 arquivos) com saída ZIP.
# Requisitos:
#   pip install streamlit pandas numpy xlsxwriter openpyxl

import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import zipfile
from datetime import datetime

st.set_page_config(page_title="Distribuição Mensal VP (NET=5) - Lote", layout="wide")

# ---------- Config ----------
MAX_FILES = 100  # Por quê: permite lotes grandes; ajuste se a memória da máquina for limitada.
ALLOWED_EXT = (".xlsx", ".xls", ".csv")

# ---------- Utilidades ----------
def parse_currency_ptbr_to_float(x):
    """'R$ 27.014.235,35' -> 27014235.35"""
    if pd.isna(x):
        return np.nan
    if isinstance(x, str):
        s = x.strip().replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
        try:
            return float(s)
        except Exception:
            return np.nan
    try:
        return float(x)
    except Exception:
        return np.nan

def to_date(x):
    return pd.to_datetime(x, errors="coerce", dayfirst=True)

def first_day_of_month(ts):
    return pd.Timestamp(ts.year, ts.month, 1)

def last_day_of_month(ts):
    return (ts + pd.offsets.MonthEnd(0)).normalize()

def days_in_range(a, b, business_days=False):
    if pd.isna(a) or pd.isna(b):
        return 0
    a = pd.Timestamp(a).normalize()
    b = pd.Timestamp(b).normalize()
    if b < a:
        a, b = b, a
    if business_days:
        return int(np.busday_count(a.date(), (b + pd.Timedelta(days=1)).date()))
    return (b - a).days + 1

def split_value_by_month(start, end, value, business_days=False):
    if pd.isna(start) or pd.isna(end) or pd.isna(value):
        return []
    if end < start:
        start, end = end, start
    meses = pd.date_range(start=first_day_of_month(start), end=first_day_of_month(end), freq="MS")
    total_dias = days_in_range(start, end, business_days=business_days)
    if total_dias <= 0:
        return []
    partes = []
    for m in meses:
        mes_ini = max(start, m)
        mes_fim = min(end, last_day_of_month(m))
        dias = days_in_range(mes_ini, mes_fim, business_days=business_days)
        if dias <= 0:
            continue
        partes.append((first_day_of_month(m), dias, value * (dias / total_dias)))
    return partes

def format_mod_label(m_value):
    """Formata M como 'MÓD. XX' (0->MÓD. 01; 1->MÓD. 02; etc.)."""
    if pd.isna(m_value):
        idx = 0
    else:
        s = str(m_value).strip().lower()
        if s in {"", "nan", "none"}:
            idx = 0
        else:
            try:
                s = s.replace(",", ".")
                idx = int(float(s))
            except Exception:
                idx = 0
    return f"MÓD. {idx + 1:02d}"

def to_excel_bytes_single(df, sheet_name="VP_por_Atividade", date_cols=None):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter", datetime_format="dd/mm/yyyy") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        wb = writer.book
        ws = writer.sheets[sheet_name]
        date_fmt = wb.add_format({"num_format": "dd/mm/yyyy"})
        if date_cols:
            for col in date_cols:
                if col in df.columns:
                    idx = df.columns.get_loc(col)
                    ws.set_column(idx, idx, 14, date_fmt)
        for i, c in enumerate(df.columns):
            try:
                width = min(max(10, int(df[c].astype(str).str.len().quantile(0.90) + 2)), 50)
            except Exception:
                width = 14
            ws.set_column(i, i, width)
    output.seek(0)
    return output.getvalue()

# ---------- Núcleo ----------
def processar(df_in: pd.DataFrame, usar_dias_uteis: bool):
    df = df_in.copy()
    df.columns = [str(c).strip() for c in df.columns]

    required = ["index", "NET", "Nome", "Início", "Término", "Custo"]
    faltando = [c for c in required if c not in df.columns]
    if faltando:
        raise ValueError(f"Colunas obrigatórias ausentes: {', '.join(faltando)}")

    df["_Inicio"]  = df["Início"].apply(to_date)
    df["_Termino"] = df["Término"].apply(to_date)
    df["_Custo"]   = df["Custo"].apply(parse_currency_ptbr_to_float).fillna(0.0)

    # Por quê: especificação define "Empreendimento + Módulo" como Nome onde index==0.
    idx_mask = pd.to_numeric(df["index"], errors="coerce") == 0
    codigo_emp_mod = ""
    if idx_mask.any():
        val = df.loc[idx_mask, "Nome"].dropna().astype(str)
        if not val.empty:
            codigo_emp_mod = val.iloc[0].strip()

    if "B" not in df.columns:
        df["B"] = pd.NA  # Por quê: manter saída consistente mesmo sem a coluna.

    # Verificar se existe coluna "M" para módulo
    if "M" not in df.columns:
        df["M"] = 0  # Valor padrão se não existir a coluna M

    net_num = pd.to_numeric(df["NET"], errors="coerce")
    df_net5 = df.loc[net_num == 5].copy()

    invalid_mask = df_net5["_Inicio"].isna() | df_net5["_Termino"].isna()
    df_net5_valid = df_net5.loc[~invalid_mask].copy()
    if df_net5_valid.empty:
        raise ValueError("Nenhuma linha com NET=5 e datas válidas ('Início' e 'Término').")

    soma_custo = df_net5_valid["_Custo"].sum()
    if soma_custo <= 0:
        n = len(df_net5_valid)
        df_net5_valid["_VP_frac"] = 1.0 / n
    else:
        df_net5_valid["_VP_frac"] = df_net5_valid["_Custo"] / soma_custo

    linhas = []
    for idx, r in df_net5_valid.iterrows():
        partes = split_value_by_month(r["_Inicio"], r["_Termino"], r["_VP_frac"], business_days=usar_dias_uteis)
        for mes_ref, dias_mes, vp_mes in partes:
            linhas.append({
                "Linha_Original": idx,
                "DataReferencia": mes_ref,
                "VP_mes": vp_mes,
                "Dias_no_Mes": dias_mes
            })
    if not linhas:
        raise ValueError("Não foi possível distribuir VP por mês para NET=5.")

    df_mes = pd.DataFrame(linhas)

    soma_id = df_mes.groupby("Linha_Original", as_index=False)["VP_mes"].sum().rename(columns={"VP_mes": "VP_somado"})
    df_mes = df_mes.merge(soma_id, on="Linha_Original", how="left")
    df_mes = df_mes.merge(
        df_net5_valid[["_VP_frac"]].rename(columns={"_VP_frac": "VP_total"}),
        left_on="Linha_Original", right_index=True, how="left"
    )
    df_mes["Dif"] = df_mes["VP_total"] - df_mes["VP_somado"]
    idx_last = df_mes.sort_values(["Linha_Original", "DataReferencia"]).groupby("Linha_Original").tail(1).index
    df_mes.loc[idx_last, "VP_mes"] = df_mes.loc[idx_last, "VP_mes"] + df_mes.loc[idx_last, "Dif"]
    df_mes.drop(columns=["VP_somado", "VP_total", "Dif"], inplace=True)

    df_out = df_mes.merge(
        df_net5_valid[["Nome", "B", "M"]],  # Incluir coluna M
        left_on="Linha_Original", right_index=True, how="left"
    )

    df_out = df_out[~df_out["VP_mes"].isna() & (df_out["VP_mes"] > 0)].copy()

    total_vp = df_out["VP_mes"].sum()
    if total_vp > 0:
        df_out["VP_mes"] = df_out["VP_mes"] / total_vp
        resid = 1.0 - df_out["VP_mes"].sum()
        if abs(resid) > 1e-12:
            df_out.iloc[-1, df_out.columns.get_loc("VP_mes")] += resid

    # Aplicar formatação do módulo (M + 1 formatado como 2 dígitos)
    df_out["Modulo_Formatado"] = df_out["M"].apply(format_mod_label)

    df_final = pd.DataFrame({
        "Empreendimento": [codigo_emp_mod] * len(df_out),
        #"Bloco": df_out["B"],
        "Módulo": df_out["Modulo_Formatado"],  # Substituir PEP por Módulo formatado
        "Atividade": df_out["Nome"],
        "VP PREVISTO": df_out["VP_mes"],
        "Mes Ano": df_out["DataReferencia"]
    })

    df_final = df_final.sort_values(["Mes Ano", "Atividade"]).reset_index(drop=True)
    return df_final, int(invalid_mask.sum())

def read_any(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        try:
            return pd.read_csv(uploaded_file)
        except Exception:
            uploaded_file.seek(0)
            return pd.read_csv(uploaded_file, sep=";")
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(uploaded_file, sheet_name=0)
    raise ValueError("Extensão não suportada.")

def build_zip(files_bytes: list[tuple[str, bytes]], report_text: str) -> bytes:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for fname, data in files_bytes:
            zf.writestr(fname, data)
        zf.writestr("_report.txt", report_text)
    buf.seek(0)
    return buf.read()

# ---------- UI ----------
st.title("Distribuição Mensal VP")
st.markdown(
    'Envie **até 100 arquivos** no modelo do Cronograma (Nexus). Cada entrada gera **um XLSX (uma única guia)**. '
    'O download final é um **ZIP**.\n\n'
    'Observação: Sempre lembrar de marcar a opção "Calcular Custos Preliminares" ao gerar o Cronograma no Nexus.'
)

# Configuração melhorada do file_uploader
uploaded_files = st.file_uploader(
    "Arraste e solte vários arquivos aqui (.xlsx, .xls ou .csv)",
    type=["xlsx", "xls", "csv"],
    accept_multiple_files=True,
    help="Você pode selecionar múltiplos arquivos ou arrastar uma pasta inteira"
)

c1, c2 = st.columns([1, 2])
with c1:
    usar_dias_uteis = st.checkbox("Considerar apenas dias úteis (seg-sex)", value=False)
with c2:
    mostrar_previa = st.checkbox("Mostrar prévias por arquivo", value=True)

st.divider()

if uploaded_files:
    # Mostrar estatísticas dos arquivos carregados
    st.success(f"✅ {len(uploaded_files)} arquivo(s) carregado(s) com sucesso!")
    
    # Listar arquivos carregados
    with st.expander("📁 Arquivos carregados", expanded=True):
        for i, file in enumerate(uploaded_files, 1):
            st.write(f"{i}. {file.name} ({file.size / 1024:.1f} KB)")
    
    if len(uploaded_files) > MAX_FILES:
        st.error(f"❌ Você enviou {len(uploaded_files)} arquivos; o limite é {MAX_FILES}. Remova alguns e tente novamente.")
    else:
        # Botão de processamento mais destacado
        st.markdown("---")
        go = st.button(
            f"🚀 Processar {len(uploaded_files)} arquivo(s)", 
            type="primary", 
            use_container_width=True
        )
        
        if go:
            results: list[tuple[str, bytes]] = []
            report_lines = []
            progress = st.progress(0)
            total = len(uploaded_files)

            # Adicionar área de status
            status_placeholder = st.empty()
            
            for i, up in enumerate(uploaded_files, start=1):
                fname = up.name
                status_placeholder.info(f"🔄 Processando {i}/{total}: {fname}")
                
                try:
                    df_in = read_any(up)
                    df_out, n_invalid = processar(df_in, usar_dias_uteis=usar_dias_uteis)
                    excel_bytes = to_excel_bytes_single(
                        df_out,
                        sheet_name="VP_por_Atividade",
                        date_cols=["Mes Ano"]
                    )
                    safe_base = fname.rsplit(".", 1)[0]
                    out_name = f"{safe_base}__VP_mensal.xlsx"
                    results.append((out_name, excel_bytes))

                    msg = f"✅ {fname} — linhas NET=5 inválidas ignoradas: {n_invalid}; soma VP={df_out['VP PREVISTO'].sum():.6f}"
                    report_lines.append(msg)

                    if mostrar_previa:
                        with st.expander(f"📊 Prévia: {fname}"):
                            st.dataframe(df_out.head(50), use_container_width=True, hide_index=True)
                            st.caption(f"Soma total VP PREVISTO (deve ser 1.0): {df_out['VP PREVISTO'].sum():.6f}")
                except Exception as e:
                    # Por quê: não interromper o lote por erro em um arquivo.
                    error_msg = f"❌ {fname} — {str(e)}"
                    report_lines.append(error_msg)
                    st.error(f"Erro em {fname}: {str(e)}")

                progress.progress(int(i * 100 / total))

            status_placeholder.empty()

            if not results and report_lines:
                st.error("❌ Falha ao processar todos os arquivos. Veja o relatório abaixo.")
                st.text("\n".join(report_lines))
            elif results:
                ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                zip_bytes = build_zip(results, "\n".join(report_lines))
                
                st.success(f"🎉 Concluído: {len(results)} arquivo(s) processado(s) com sucesso!")
                
                # Botão de download mais destacado
                st.download_button(
                    label="📥 Baixar ZIP com todos os arquivos processados",
                    data=zip_bytes,
                    file_name=f"VP_mensal_lote_{ts}.zip",
                    mime="application/zip",
                    type="primary",
                    use_container_width=True
                )
                
                with st.expander("📋 Relatório detalhado de processamento"):
                    st.text("\n".join(report_lines))
else:
    st.info("""
    📌 **Como usar:**
    1. Arraste e solte vários arquivos Excel/CSV na área acima
    2. Ou clique em "Browse files" para selecionar múltiplos arquivos (Ctrl+Click)
    3. Ajuste as configurações se necessário
    4. Clique no botão "Processar" para gerar o ZIP
    """)

# Adicionar dicas de uso
with st.expander("💡 Dicas para carregar múltiplos arquivos"):
    st.markdown("""
    - **Windows/Linux**: Selecione múltiplos arquivos com `Ctrl + Click`
    - **Mac**: Selecione múltiplos arquivos com `Cmd + Click`  
    - **Arraste e solte**: Você pode arrastar uma pasta inteira ou vários arquivos selecionados
    - **Formatos suportados**: .xlsx, .xls, .csv
    - **Limite**: Máximo de 100 arquivos por vez
    """)
