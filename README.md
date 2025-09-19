# Distribuição Mensal VP (NET=5) — Lote (Streamlit)

Processa **até 100 planilhas** de uma vez (CSV/XLS/XLSX), calcula o **VP** apenas para **NET = 5**, **distribui por mês** entre `Início` e `Término` (opcional: só dias úteis), e exporta **um XLSX por arquivo** em um **ZIP** com relatório.

## ✨ Principais recursos
- Upload múltiplo (até **100 arquivos** por lote).
- CSV `,` ou `;` automaticamente; Excel (primeira aba).
- Cálculo e distribuição mensal do VP (NET=5).
- Normalização para total **= 100%** por arquivo.
- Saída **padronizada** (colunas e ordem exigidas).
- Prévia por arquivo (opcional).
- Download como **ZIP** com `_report.txt`.

---

## ✅ Pré-requisitos
- Python 3.9+  
- Sistema com build C básico (pandas/openpyxl/xlsxwriter)

### Dependências
```bash
pip install streamlit pandas numpy xlsxwriter openpyxl
