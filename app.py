# app.py
# Streamlit — processamento em lote otimizado para Cloud

import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import zipfile
from datetime import datetime

# ---------- CONFIGURAÇÕES INICIAIS OTIMIZADAS ----------
st.set_page_config(
    page_title="Distribuição Mensal VP (NET=5) - Lote",
    page_icon="📊",
    layout="wide"
)

# Cache para evitar reprocessamento
@st.cache_data(show_spinner=False, ttl=3600)
def load_config():
    return {
        "MAX_FILES": 50,  # Reduzido para melhor performance
        "CHUNK_SIZE": 10000,  # Processar em chunks se necessário
    }

config = load_config()

# ---------- IMPORTS LEVES E LAZY LOADING ----------
def safe_imports():
    """Importa bibliotecas pesadas apenas quando necessário"""
    try:
        import xlsxwriter
        return True
    except ImportError:
        st.error("Bibliotecas necessárias não instaladas. Execute: pip install xlsxwriter openpyxl")
        return False

# ---------- FUNÇÕES OTIMIZADAS ----------
@st.cache_data
def parse_currency_ptbr_to_float(x):
    """Versão vetorizada e otimizada para parsing de moeda"""
    if pd.isna(x):
        return np.nan
    
    if isinstance(x, (int, float)):
        return float(x)
    
    if isinstance(x, str):
        # Remove caracteres não numéricos de forma mais eficiente
        s = x.strip().replace('R$', '').replace(' ', '')
        # Encontra a última vírgula (separador decimal)
        if ',' in s:
            parts = s.rsplit(',', 1)
            if len(parts) == 2:
                int_part = parts[0].replace('.', '')
                dec_part = parts[1]
                s = f"{int_part}.{dec_part}"
            else:
                s = s.replace(',', '').replace('.', '')
        else:
            s = s.replace('.', '')
        
        try:
            return float(s) if s else np.nan
        except (ValueError, TypeError):
            return np.nan
    
    return np.nan

@st.cache_data
def to_date_optimized(series):
    """Versão otimizada para conversão de datas"""
    return pd.to_datetime(series, errors='coerce', dayfirst=True, infer_datetime_format=True)

def first_day_of_month(ts):
    return ts.replace(day=1)

def last_day_of_month(ts):
    return (ts.replace(day=28) + pd.Timedelta(days=4)).replace(day=1) - pd.Timedelta(days=1)

def days_in_range_optimized(start_dates, end_dates, business_days=False):
    """Versão vetorizada para cálculo de dias"""
    mask = ~(pd.isna(start_dates) | pd.isna(end_dates))
    result = pd.Series(0, index=start_dates.index)
    
    if business_days:
        # Implementação mais eficiente para dias úteis
        for idx in start_dates[mask].index:
            start = start_dates[idx].normalize()
            end = end_dates[idx].normalize()
            if end < start:
                start, end = end, start
            result[idx] = np.busday_count(start.date(), (end + pd.Timedelta(days=1)).date())
    else:
        # Cálculo direto para dias corridos
        valid_starts = start_dates[mask]
        valid_ends = end_dates[mask]
        result[mask] = ((valid_ends - valid_starts).dt.days + 1).clip(lower=0)
    
    return result

def split_value_by_month_batch(df_batch, business_days=False):
    """Processamento em lote otimizado"""
    results = []
    
    for idx, row in df_batch.iterrows():
        start, end, value = row['_Inicio'], row['_Termino'], row['_VP_frac']
        
        if pd.isna(start) or pd.isna(end) or pd.isna(value):
            continue
            
        if end < start:
            start, end = end, start
            
        # Gera meses de forma mais eficiente
        current = first_day_of_month(start)
        end_month = first_day_of_month(end)
        
        total_dias = days_in_range_optimized(
            pd.Series([start]), pd.Series([end]), business_days
        ).iloc[0]
        
        if total_dias <= 0:
            continue
            
        while current <= end_month:
            mes_ini = max(start, current)
            mes_fim = min(end, last_day_of_month(current))
            
            dias = days_in_range_optimized(
                pd.Series([mes_ini]), pd.Series([mes_fim]), business_days
            ).iloc[0]
            
            if dias > 0:
                results.append({
                    'Linha_Original': idx,
                    'DataReferencia': current,
                    'VP_mes': value * (dias / total_dias),
                    'Dias_no_Mes': dias
                })
            
            current += pd.offsets.MonthBegin(1)
    
    return results

@st.cache_data
def format_mod_label_series(m_series):
    """Versão vetorizada para formatação de módulos"""
    def format_single(m_value):
        if pd.isna(m_value):
            return "MÓD. 01"
        
        s = str(m_value).strip().lower()
        if s in {"", "nan", "none"}:
            return "MÓD. 01"
        
        try:
            s_clean = s.replace(",", ".")
            idx = int(float(s_clean))
            return f"MÓD. {idx + 1:02d}"
        except (ValueError, TypeError):
            return "MÓD. 01"
    
    return m_series.apply(format_single)

# ---------- PROCESSAMENTO PRINCIPAL OTIMIZADO ----------
def processar_otimizado(df_in: pd.DataFrame, usar_dias_uteis: bool):
    """Versão otimizada do processamento principal"""
    # Cria cópia e limpa colunas
    df = df_in.copy()
    df.columns = [str(c).strip() for c in df.columns]
    
    # Verifica colunas obrigatórias
    required = ["index", "NET", "Nome", "Início", "Término", "Custo"]
    faltando = [c for c in required if c not in df.columns]
    if faltando:
        raise ValueError(f"Colunas obrigatórias ausentes: {', '.join(faltando)}")
    
    # Conversões otimizadas
    df["_Inicio"] = to_date_optimized(df["Início"])
    df["_Termino"] = to_date_optimized(df["Término"])
    
    # Parsing de custo otimizado
    custo_series = df["Custo"].astype(str)
    df["_Custo"] = custo_series.apply(parse_currency_ptbr_to_float).fillna(0.0)
    
    # Encontra código do empreendimento
    idx_mask = pd.to_numeric(df["index"], errors="coerce") == 0
    codigo_emp_mod = ""
    if idx_mask.any():
        val = df.loc[idx_mask, "Nome"].dropna().astype(str)
        if not val.empty:
            codigo_emp_mod = val.iloc[0].strip()
    
    # Preenche colunas opcionais
    if "B" not in df.columns:
        df["B"] = pd.NA
    
    if "M" not in df.columns:
        df["M"] = 0
    
    # Filtra NET=5
    net_num = pd.to_numeric(df["NET"], errors="coerce")
    df_net5 = df.loc[net_num == 5].copy()
    
    # Remove linhas inválidas
    invalid_mask = df_net5["_Inicio"].isna() | df_net5["_Termino"].isna()
    df_net5_valid = df_net5.loc[~invalid_mask].copy()
    
    if df_net5_valid.empty:
        raise ValueError("Nenhuma linha com NET=5 e datas válidas ('Início' e 'Término').")
    
    # Calcula fração VP
    soma_custo = df_net5_valid["_Custo"].sum()
    if soma_custo <= 0:
        n = len(df_net5_valid)
        df_net5_valid["_VP_frac"] = 1.0 / n
    else:
        df_net5_valid["_VP_frac"] = df_net5_valid["_Custo"] / soma_custo
    
    # Processamento em lote dos meses
    linhas = split_value_by_month_batch(df_net5_valid, business_days=usar_dias_uteis)
    
    if not linhas:
        raise ValueError("Não foi possível distribuir VP por mês para NET=5.")
    
    # Cria DataFrame de meses
    df_mes = pd.DataFrame(linhas)
    
    # Ajuste de precisão (mantido da versão original)
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
    
    # Merge com dados originais
    df_out = df_mes.merge(
        df_net5_valid[["Nome", "B", "M"]],
        left_on="Linha_Original", right_index=True, how="left"
    )
    
    # Filtra valores válidos
    df_out = df_out[~df_out["VP_mes"].isna() & (df_out["VP_mes"] > 0)].copy()
    
    # Normalização final
    total_vp = df_out["VP_mes"].sum()
    if total_vp > 0:
        df_out["VP_mes"] = df_out["VP_mes"] / total_vp
        resid = 1.0 - df_out["VP_mes"].sum()
        if abs(resid) > 1e-12:
            df_out.iloc[-1, df_out.columns.get_loc("VP_mes")] += resid
    
    # Formata módulo
    df_out["Modulo_Formatado"] = format_mod_label_series(df_out["M"])
    
    # DataFrame final
    df_final = pd.DataFrame({
        "Empreendimento": [codigo_emp_mod] * len(df_out),
        "Módulo": df_out["Modulo_Formatado"],
        "Atividade": df_out["Nome"],
        "VP PREVISTO": df_out["VP_mes"],
        "Mes Ano": df_out["DataReferencia"]
    })
    
    df_final = df_final.sort_values(["Mes Ano", "Atividade"]).reset_index(drop=True)
    return df_final, int(invalid_mask.sum())

# ---------- LEITURA E ESCRITA OTIMIZADAS ----------
@st.cache_data(show_spinner=False)
def read_any_optimized(uploaded_file):
    """Leitura otimizada de arquivos"""
    name = uploaded_file.name.lower()
    
    try:
        if name.endswith(".csv"):
            # Tenta detectar encoding e separador
            sample = uploaded_file.read(1024).decode('utf-8', errors='ignore')
            uploaded_file.seek(0)
            
            sep = ';' if ';' in sample else ','
            return pd.read_csv(uploaded_file, sep=sep, encoding='utf-8', engine='python')
        
        elif name.endswith((".xlsx", ".xls")):
            # Lê apenas dados, ignora formatação
            return pd.read_excel(
                uploaded_file, 
                sheet_name=0, 
                engine='openpyxl',
                dtype=str,  # Lê tudo como string e converte depois
                na_values=['', 'NULL', 'null', 'NaN', 'N/A']
            )
    
    except Exception as e:
        raise ValueError(f"Erro ao ler arquivo {uploaded_file.name}: {str(e)}")

def to_excel_bytes_optimized(df, sheet_name="VP_por_Atividade"):
    """Geração otimizada de Excel"""
    output = BytesIO()
    
    # Configurações para performance
    with pd.ExcelWriter(
        output, 
        engine='xlsxwriter',
        options={'strings_to_numbers': True, 'strings_to_urls': False}
    ) as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Formatação básica
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        
        # Formato de data
        date_format = workbook.add_format({'num_format': 'dd/mm/yyyy'})
        
        # Ajusta largura das colunas
        for idx, col in enumerate(df.columns):
            max_len = max(
                df[col].astype(str).str.len().max(),
                len(str(col))
            ) + 2
            worksheet.set_column(idx, idx, min(max_len, 50))
            
            # Aplica formato de data se for datetime
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                worksheet.set_column(idx, idx, 14, date_format)
    
    output.seek(0)
    return output.getvalue()

def build_zip_optimized(files_bytes):
    """Criação otimizada de ZIP"""
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for fname, data in files_bytes:
            zf.writestr(fname, data)
    buf.seek(0)
    return buf.getvalue()

# ---------- INTERFACE DO USUÁRIO OTIMIZADA ----------
def main():
    # Verifica imports
    if not safe_imports():
        return
    
    # Header
    st.title("🚀 Distribuição Mensal VP - Otimizado")
    st.markdown("""
    **Processamento em lote otimizado para nuvem**  
    Envie múltiplos arquivos do Cronograma (Nexus) e receba um ZIP com os resultados.
    """)
    
    # Configurações
    with st.sidebar:
        st.header("⚙️ Configurações")
        usar_dias_uteis = st.checkbox("Considerar apenas dias úteis", value=False)
        mostrar_previa = st.checkbox("Mostrar prévias", value=False)
        st.markdown("---")
        st.info("💡 **Dica:** Arquivos menores processam mais rápido!")
    
    # Upload de arquivos
    uploaded_files = st.file_uploader(
        "📁 Arraste ou selecione os arquivos",
        type=["xlsx", "xls", "csv"],
        accept_multiple_files=True,
        help="Máximo 50 arquivos por vez"
    )
    
    # Processamento
    if uploaded_files:
        st.success(f"✅ {len(uploaded_files)} arquivo(s) carregado(s)")
        
        # Lista arquivos
        with st.expander("📋 Arquivos carregados", expanded=False):
            for i, file in enumerate(uploaded_files, 1):
                st.write(f"{i}. {file.name} ({file.size / 1024:.1f} KB)")
        
        if len(uploaded_files) > config["MAX_FILES"]:
            st.error(f"❌ Limite de {config['MAX_FILES']} arquivos excedido")
            return
        
        # Botão de processamento
        if st.button("🚀 Processar Arquivos", type="primary", use_container_width=True):
            process_files(uploaded_files, usar_dias_uteis, mostrar_previa)
    
    else:
        show_instructions()

def process_files(uploaded_files, usar_dias_uteis, mostrar_previa):
    """Processa os arquivos de forma otimizada"""
    results = []
    report_lines = []
    total_files = len(uploaded_files)
    
    # Barra de progresso
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, uploaded_file in enumerate(uploaded_files):
        try:
            # Atualiza status
            status_text.info(f"🔄 Processando {i+1}/{total_files}: {uploaded_file.name}")
            
            # Lê e processa
            df_in = read_any_optimized(uploaded_file)
            df_out, n_invalid = processar_otimizado(df_in, usar_dias_uteis)
            
            # Gera Excel
            excel_bytes = to_excel_bytes_optimized(df_out)
            
            # Nome do arquivo de saída
            safe_name = uploaded_file.name.rsplit(".", 1)[0]
            out_name = f"{safe_name}__VP_mensal.xlsx"
            results.append((out_name, excel_bytes))
            
            # Registra sucesso
            msg = f"✅ {uploaded_file.name} - {n_invalid} inválidas - VP: {df_out['VP PREVISTO'].sum():.6f}"
            report_lines.append(msg)
            
            # Prévia se solicitado
            if mostrar_previa:
                with st.expander(f"📊 Prévia: {uploaded_file.name}"):
                    st.dataframe(df_out.head(20), use_container_width=True)
                    st.caption(f"Total VP: {df_out['VP PREVISTO'].sum():.6f}")
        
        except Exception as e:
            error_msg = f"❌ {uploaded_file.name} - ERRO: {str(e)}"
            report_lines.append(error_msg)
            st.error(f"Erro em {uploaded_file.name}: {str(e)}")
        
        # Atualiza progresso
        progress_bar.progress((i + 1) / total_files)
    
    # Finalização
    status_text.empty()
    progress_bar.empty()
    
    if results:
        show_results(results, report_lines)
    else:
        st.error("❌ Nenhum arquivo processado com sucesso")

def show_results(results, report_lines):
    """Exibe resultados e botão de download"""
    # Gera ZIP
    with st.spinner("📦 Gerando arquivo ZIP..."):
        zip_bytes = build_zip_optimized(results)
    
    # Timestamp para nome do arquivo
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Interface de sucesso
    st.success(f"🎉 Processamento concluído: {len(results)} arquivo(s) gerado(s)")
    
    # Botão de download
    st.download_button(
        label="📥 Baixar ZIP com Resultados",
        data=zip_bytes,
        file_name=f"vp_mensal_lote_{ts}.zip",
        mime="application/zip",
        type="primary",
        use_container_width=True
    )
    
    # Relatório
    with st.expander("📋 Relatório de Processamento"):
        st.text("\n".join(report_lines))

def show_instructions():
    """Mostra instruções de uso"""
    st.info("""
    ## 📌 Como usar:
    
    1. **Prepare seus arquivos** no formato do Cronograma Nexus
    2. **Envie os arquivos** arrastando ou clicando na área acima
    3. **Ajuste as configurações** na barra lateral se necessário
    4. **Clique em Processar** para gerar os resultados
    5. **Baixe o ZIP** com todos os arquivos processados
    
    ### ✅ Formatos suportados:
    - Excel (.xlsx, .xls)
    - CSV (.csv)
    
    ### ⚡ Dicas de performance:
    - Limite a 10-20 arquivos por vez para melhor velocidade
    - Arquivos menores processam mais rápido
    - Use a opção de dias úteis apenas quando necessário
    """)

# ---------- EXECUÇÃO ----------
if __name__ == "__main__":
    main()