import streamlit as st
import pandas as pd
import clickhouse_connect
import datetime
import matplotlib.pyplot as plt

# Configuração da página
st.set_page_config(page_title="Análise de Commits", page_icon="📊", layout="wide")
st.title("Análise de Commits por Sprint")

# Função para conectar ao ClickHouse
def connect_to_clickhouse():
    client = clickhouse_connect.get_client(
        host="gowlim80fx.us-east1.gcp.clickhouse.cloud",
        port=8443,
        username="default",
        password="JBhX.Pm6OJVO6",
        secure=True
    )
    return client

# Função para converter resultado da query para DataFrame
def query_to_dataframe(result):
    column_names = [col[0] for col in result.column_names]
    data = result.result_rows
    return pd.DataFrame(data, columns=column_names)

# Interface do usuário para filtros de data
st.sidebar.header("Filtros de Data")

# Data padrão final (15 de março de 2025)
default_end_date = datetime.date(2025, 3, 15)

# Data padrão inicial (5 de março de 2025 - 10 dias antes)
default_start_date = datetime.date(2025, 3, 5)

# Seleção de datas
start_date = st.sidebar.date_input(
    "Data Inicial da Sprint",
    value=default_start_date
)

end_date = st.sidebar.date_input(
    "Data Final da Sprint",
    value=default_end_date
)

# Hora limite para o final da sprint
end_time = st.sidebar.time_input(
    "Hora limite (final da sprint)",
    value=datetime.time(4, 59, 59)
)

# Formatação das datas para as queries
start_datetime = f"{start_date} 00:00:00"
end_datetime = f"{end_date} {end_time}"

# Executar queries quando o botão for pressionado
if st.sidebar.button("Executar Análise"):
    try:
        # Conectar ao ClickHouse
        client = connect_to_clickhouse()
        
        # Mostrar informação sobre o período analisado
        st.info("As datas estão em GMT, considere sempre colocar 3 horas na frente a data final para São Paulo, Brasil.")
        st.info(f"Analisando commits de {start_datetime} até {end_datetime}")
        
        # Separar a interface em três seções usando abas
        tab1, tab2, tab3 = st.tabs([
            "Commits Dentro do Prazo", 
            "Alunos Sem Commits na Sprint", 
            "Commits Após o Prazo"
        ])
        
        # Query 1: Relatório de autores que atuaram na Sprint dentro do prazo
        with tab1:
            st.header("Autores com Commits Dentro do Prazo")
            
            query1 = f"""
            SELECT 
                t1.repo_name,
                t1.author,
                parseDateTimeBestEffort(t1.date) AS commit_date,
                t1.message
            FROM commits t1
            JOIN (
                SELECT 
                    repo_name, 
                    MAX(parseDateTimeBestEffort(date)) AS max_date
                FROM commits
                WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                AND parseDateTimeBestEffort(date) >= '{start_datetime}'
                AND parseDateTimeBestEffort(date) <= '{end_datetime}'
                AND author NOT IN ('Inteli Hub', 'José Romualdo')
                GROUP BY repo_name
            ) t2 ON t1.repo_name = t2.repo_name AND parseDateTimeBestEffort(t1.date) = t2.max_date
            WHERE (t1.repo_name ILIKE '%INTERNO%' OR t1.repo_name ILIKE '%PUBLICO%')
            AND parseDateTimeBestEffort(t1.date) >= '{start_datetime}'
            AND parseDateTimeBestEffort(t1.date) <= '{end_datetime}'
            AND t1.author NOT IN ('Inteli Hub', 'José Romualdo')
            ORDER BY t1.repo_name
            """
            
            result1 = client.query(query1)
            
            if result1.result_rows:
                # Criar DataFrame com os resultados
                df1 = query_to_dataframe(result1)
                
                # Formatar a coluna de data para facilitar a leitura
                if 'commit_date' in df1.columns:
                    # Converter para timezone de São Paulo
                    df1['commit_date'] = pd.to_datetime(df1['commit_date']).dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
                    df1['commit_date'] = df1['commit_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Mostrar a tabela
                st.dataframe(df1, use_container_width=True)
                st.success(f"Total de {len(df1)} commits dentro do prazo")
                
                # Query adicional para contar commits por repositório
                query_repos = f"""
                SELECT 
                    repo_name,
                    COUNT(*) as count
                FROM commits
                WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                AND parseDateTimeBestEffort(date) >= '{start_datetime}'
                AND parseDateTimeBestEffort(date) <= '{end_datetime}'
                AND author NOT IN ('Inteli Hub', 'José Romualdo')
                GROUP BY repo_name
                ORDER BY count DESC
                LIMIT 10
                """
                
                result_repos = client.query(query_repos)
                df_repos = query_to_dataframe(result_repos)
                
                # Renomear a coluna 'count' para 'num_commits' se existir
                if 'count' in df_repos.columns:
                    df_repos = df_repos.rename(columns={'count': 'num_commits'})
                
                # Query adicional para contar commits por autor
                query_authors = f"""
                SELECT 
                    author,
                    COUNT(*) as count
                FROM commits
                WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                AND parseDateTimeBestEffort(date) >= '{start_datetime}'
                AND parseDateTimeBestEffort(date) <= '{end_datetime}'
                AND author NOT IN ('Inteli Hub', 'José Romualdo')
                GROUP BY author
                ORDER BY count DESC
                LIMIT 10
                """
                
                result_authors = client.query(query_authors)
                df_authors = query_to_dataframe(result_authors)
                
                # Renomear a coluna 'count' para 'num_commits' se existir
                if 'count' in df_authors.columns:
                    df_authors = df_authors.rename(columns={'count': 'num_commits'})
                
                # Exibir os gráficos em colunas
                st.subheader("Análise de Commits na Sprint")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Top 10 Repositórios com Mais Commits")
                    if not df_repos.empty:
                        # Mostrar os nomes das colunas para diagnóstico
                        st.text(f"Colunas disponíveis no DataFrame de repositórios: {df_repos.columns.tolist()}")
                        
                        # Verificar quais colunas temos e criar gráfico de acordo
                        if len(df_repos.columns) >= 2:
                            # Pegando a primeira coluna como nome do repositório e a segunda como contagem
                            repo_col = df_repos.columns[0]
                            count_col = df_repos.columns[1]
                            
                            # Criar uma cópia com nomes de colunas consistentes
                            df_plot = df_repos.copy()
                            df_plot = df_plot.rename(columns={
                                repo_col: 'repo_name', 
                                count_col: 'num_commits'
                            })
                            
                            # Ordenar por número de commits (do maior para o menor)
                            df_plot = df_plot.sort_values('num_commits', ascending=False)
                            
                            # Exibir dados em uma tabela
                            st.dataframe(df_plot)
                            
                            # Criar plot manualmente para maior controle
                            fig, ax = plt.subplots(figsize=(10, 6))
                            ax.barh(df_plot['repo_name'], df_plot['num_commits'])
                            ax.set_xlabel('Número de Commits')
                            ax.set_ylabel('Repositório')
                            ax.set_title('Top 10 Repositórios com Mais Commits')
                            plt.tight_layout()
                            st.pyplot(fig)
                        else:
                            st.error(f"Estrutura de dados inesperada para repositórios. Colunas: {df_repos.columns.tolist()}")
                    else:
                        st.info("Sem dados de repositórios para exibir")
                
                with col2:
                    st.subheader("Top 10 Autores Mais Ativos")
                    if not df_authors.empty:
                        # Mostrar os nomes das colunas para diagnóstico
                        st.text(f"Colunas disponíveis no DataFrame de autores: {df_authors.columns.tolist()}")
                        
                        # Verificar quais colunas temos e criar gráfico de acordo
                        if len(df_authors.columns) >= 2:
                            # Pegando a primeira coluna como nome do autor e a segunda como contagem
                            author_col = df_authors.columns[0]
                            count_col = df_authors.columns[1]
                            
                            # Criar uma cópia com nomes de colunas consistentes
                            df_plot = df_authors.copy()
                            df_plot = df_plot.rename(columns={
                                author_col: 'author', 
                                count_col: 'num_commits'
                            })
                            
                            # Ordenar por número de commits (do maior para o menor)
                            df_plot = df_plot.sort_values('num_commits', ascending=False)
                            
                            # Exibir dados em uma tabela
                            st.dataframe(df_plot)
                            
                            # Criar plot manualmente para maior controle
                            fig, ax = plt.subplots(figsize=(10, 6))
                            ax.barh(df_plot['author'], df_plot['num_commits'])
                            ax.set_xlabel('Número de Commits')
                            ax.set_ylabel('Autor')
                            ax.set_title('Top 10 Autores com Mais Commits')
                            plt.tight_layout()
                            st.pyplot(fig)
                        else:
                            st.error(f"Estrutura de dados inesperada para autores. Colunas: {df_authors.columns.tolist()}")
                    else:
                        st.info("Sem dados de autores para exibir")
            else:
                st.warning("Nenhum resultado encontrado para commits dentro do prazo.")
        
        # Query 2: Alunos sem commits na sprint
        with tab2:
            st.header("Alunos Sem Commits na Sprint")
            
            query2 = f"""
            SELECT DISTINCT author
            FROM commits
            WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
            AND author NOT IN ('Inteli Hub', 'José Romualdo')
            AND parseDateTimeBestEffort(date) >= '{end_datetime}'
            AND author NOT IN (
                SELECT DISTINCT author
                FROM commits
                WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                AND parseDateTimeBestEffort(date) >= '{start_datetime}'
                AND parseDateTimeBestEffort(date) < '{end_datetime}'
            )
            ORDER BY author
            """
            
            result2 = client.query(query2)
            
            if result2.result_rows:
                # Criar DataFrame com os resultados
                df2 = query_to_dataframe(result2)
                
                # Mostrar a tabela
                st.dataframe(df2, use_container_width=True)
                st.error(f"Total de {len(df2)} alunos sem commits na sprint")
            else:
                st.success("Todos os alunos fizeram commits durante a sprint!")
        
        # Query 3: Repositórios com commits posteriores ao prazo e seus colaboradores
        with tab3:
            st.header("Repositórios com Commits Após o Prazo e Seus Colaboradores")
            
            # Primeiro, obter todos os commits realizados após o prazo
            query3_commits = f"""
            SELECT 
                author,
                repo_name,
                parseDateTimeBestEffort(date) AS commit_date,
                message
            FROM commits
            WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
            AND author NOT IN ('Inteli Hub', 'José Romualdo')
            AND parseDateTimeBestEffort(date) >= '{end_datetime}'
            AND author IN (
                -- Autores que não têm commits dentro do prazo
                SELECT DISTINCT author
                FROM commits
                WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                AND author NOT IN ('Inteli Hub', 'José Romualdo')
                AND parseDateTimeBestEffort(date) >= '{end_datetime}'
                AND author NOT IN (
                    SELECT DISTINCT author
                    FROM commits
                    WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                    AND parseDateTimeBestEffort(date) >= '{start_datetime}'
                    AND parseDateTimeBestEffort(date) < '{end_datetime}'
                )
            )
            ORDER BY parseDateTimeBestEffort(date) DESC
            """
            
            result3_commits = client.query(query3_commits)
            
            if result3_commits.result_rows:
                # Criar DataFrame com os resultados
                df_commits = query_to_dataframe(result3_commits)
                
                # Verificar se temos as colunas esperadas
                if len(df_commits.columns) >= 4:
                    # Garantir nomes de colunas consistentes
                    if 'commit_date' in df_commits.columns:
                        # Converter para timezone de São Paulo
                        df_commits['commit_date'] = pd.to_datetime(df_commits['commit_date']).dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
                        df_commits['commit_date'] = df_commits['commit_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Exibir a lista de todos os commits após o prazo
                    st.subheader("Lista de todos os commits realizados após o prazo")
                    st.dataframe(df_commits, use_container_width=True)
                    
                # Agora, consulta para obter repositórios e colaboradores
                query3 = f"""
                WITH repos AS (
                    -- Repositórios com commits após o prazo (de autores que não cumpriram o prazo)
                    SELECT DISTINCT 
                        repo_name
                    FROM commits
                    WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                    AND author NOT IN ('Inteli Hub', 'José Romualdo')
                    AND parseDateTimeBestEffort(date) >= '{end_datetime}'
                    AND author IN (
                        -- Autores que não têm commits dentro do prazo
                        SELECT DISTINCT author
                        FROM commits
                        WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                        AND parseDateTimeBestEffort(date) >= '{end_datetime}'
                        AND author NOT IN (
                            SELECT DISTINCT author
                            FROM commits
                            WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                            AND parseDateTimeBestEffort(date) >= '{start_datetime}'
                            AND parseDateTimeBestEffort(date) < '{end_datetime}'
                        )
                    )
                )
                -- Obter todos os autores desses repositórios, independente de quando fizeram commits
                SELECT DISTINCT
                    c.repo_name,
                    c.author
                FROM commits c
                JOIN repos r ON c.repo_name = r.repo_name
                WHERE c.author NOT IN ('Inteli Hub', 'José Romualdo')
                ORDER BY c.repo_name, c.author
                """
                
                result3 = client.query(query3)
                
                if result3.result_rows:
                    # Criar DataFrame com os resultados
                    df3 = query_to_dataframe(result3)
                    
                    # Mostrar as colunas disponíveis (para debug)
                    st.text(f"Colunas disponíveis: {df3.columns.tolist()}")
                    
                    # Verificar quais colunas estão disponíveis
                    if len(df3.columns) >= 2:
                        # Usar os primeiros dois campos para repositório e autor
                        repo_col = df3.columns[0]
                        author_col = df3.columns[1]
                        
                        # Renomear para clareza
                        df3 = df3.rename(columns={
                            repo_col: 'Repositório',
                            author_col: 'Autor'
                        })
                        
                        # Exibir a tabela completa primeiro
                        st.subheader("Lista completa de colaboradores por repositório")
                        st.dataframe(df3, use_container_width=True)
                        
                        # Agrupar por repositório
                        repos = df3['Repositório'].unique()
                        
                        # Para cada repositório, exibir a lista de autores
                        for repo in repos:
                            # Filtrar apenas os autores deste repositório
                            authors = df3[df3['Repositório'] == repo]['Autor'].tolist()
                            
                            # Exibir o nome do repositório e a lista de autores
                            st.markdown(f"### {repo}")
                            st.markdown(f"**Total de {len(authors)} colaboradores**")
                            
                            # Exibir a lista de autores como texto formatado
                            author_list = ", ".join(authors)
                            st.markdown(f"**Colaboradores:** {author_list}")
                            
                            # Adicionar uma linha divisória
                            st.markdown("---")
                        
                        # Exibir resumo
                        st.warning(f"Total de {len(repos)} repositórios que tiveram commits após o prazo")
                    else:
                        st.error(f"Estrutura de dados inesperada. Colunas: {df3.columns.tolist()}")
                else:
                    st.warning("Não foi possível obter a lista de colaboradores por repositório.")
            else:
                st.success("Nenhum repositório tem commits após o prazo da sprint!")
                
    except Exception as e:
        st.error(f"Erro ao conectar ou consultar o ClickHouse: {str(e)}")
        st.code(str(e))
        
        # Mostrar detalhes adicionais para ajudar no debug
        import traceback
        st.code(traceback.format_exc())
else:
    # Mensagem inicial antes da execução
    st.info("Selecione o período da sprint no menu lateral e clique em 'Executar Análise' para visualizar os resultados.")
    
    # Explicação das três consultas
    st.subheader("O que este aplicativo analisa:")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("#### Commits Dentro do Prazo")
        st.write("Mostra o último commit de cada repositório feito por cada autor dentro do prazo da sprint.")
    
    with col2:
        st.markdown("#### Alunos Sem Commits")
        st.write("Lista os alunos que não fizeram commits durante o período da sprint (mas têm commits posteriores).")
    
    with col3:
        st.markdown("#### Repositórios Após o Prazo")
        st.write("Mostra repositórios com commits após o prazo e lista os colaboradores que não cumpriram o prazo da sprint.")

# Rodapé com informações
st.sidebar.markdown("---")
st.sidebar.info("Desenvolvido para análise de commits em repositórios INTERNO/PUBLICO.")