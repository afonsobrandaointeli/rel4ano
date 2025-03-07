import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, date

def main():
    st.title("Relatório de Commits (repos INTERNO)")

    # URL do banco de dados fornecida
    db_url = "postgresql://getcommits_user:X4oeIBBTdnpQiyI5x0XcmdVHcze5ooY1@dpg-cpg3p8mct0pc73d6f8m0-a.oregon-postgres.render.com/getcommits"
    
    # Cria engine para conectar no banco
    engine = create_engine(db_url)
    
    # Query para ler dados necessários
    query = """
        SELECT 
            date,
            author,
            email,
            url
        FROM commits
        WHERE repo_name LIKE '%%INTERNO%%'
          AND author != 'Inteli Hub'
    """
    df = pd.read_sql(query, engine)
    
    # Converte a coluna 'date' para datetime (caso ainda não esteja)
    df['date'] = pd.to_datetime(df['date'])

    # Ordena por data (opcional, mas útil)
    df = df.sort_values(by='date')

    st.write("### Dados brutos (todos os commits filtrados do banco):")
    st.dataframe(df)

    st.write("---")
    st.write("### Filtro por Período")

    # Define datas mínimas e máximas para filtrar
    min_date = df['date'].min().date() if not df.empty else date(2020,1,1)
    max_date = df['date'].max().date() if not df.empty else date.today()

    # Inputs do Streamlit para data inicial e final
    start_date = st.date_input("Data Inicial", value=min_date, min_value=min_date, max_value=max_date)
    end_date = st.date_input("Data Final", value=max_date, min_value=min_date, max_value=max_date)

    # Filtra o DataFrame com base no período selecionado
    if start_date > end_date:
        st.error("A data inicial não pode ser maior que a data final.")
    else:
        filtered_df = df[(df['date'] >= pd.to_datetime(start_date)) & 
                         (df['date'] <= pd.to_datetime(end_date))]

        # Exibe o total de commits nesse período
        total_commits = len(filtered_df)
        st.write(f"**Total de commits no período:** {total_commits}")

        # Agrupa por autor e conta o número de commits
        commits_por_author = (
            filtered_df
            .groupby('author')
            .size()
            .reset_index(name='total_commits')
            .sort_values(by='total_commits', ascending=False)
        )

        st.write("### Quantidade de Commits por Autor")
        st.dataframe(commits_por_author)

        st.write("### Detalhamento (lista de commits filtrados)")
        st.dataframe(filtered_df.reset_index(drop=True))

if __name__ == "__main__":
    main()
