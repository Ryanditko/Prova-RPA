"""
Projeto RPA - Coleta de Dados Meteorológicos
API Escolhida: OpenWeatherMap API (https://openweathermap.org/api)

Justificativa da escolha:
- API gratuita com boa documentação
- Dados meteorológicos em tempo real de diversas cidades
- Fácil integração com Python
- Retorna dados estruturados em JSON
- Suporta múltiplos idiomas (incluindo português)

Nota: API Key incluída diretamente no código para facilitar a correção da prova
"""

import requests
import json
import sqlite3
from datetime import datetime


def carregar_configuracoes():
    """Carrega a API key (configurada diretamente no código para entrega da prova)"""
    # API Key do OpenWeatherMap (gratuita)
    # Para obter uma nova chave: https://openweathermap.org/api
    api_key = "3a5eb8cc097c4eb15bf47deb31c0f8a6"
    
    if not api_key:
        raise ValueError("API_KEY não configurada")
    
    print(f"✓ API Key carregada com sucesso!")
    return api_key


def obter_dados_clima(cidade, api_key):
    """
    Realiza requisição à API OpenWeatherMap para obter dados do clima
    
    Args:
        cidade (str): Nome da cidade
        api_key (str): Chave da API
        
    Returns:
        dict: Dados do clima ou None em caso de erro
    """
    url = f"https://api.openweathermap.org/data/2.5/weather?q={cidade}&appid={api_key}&units=metric&lang=pt_br"
    
    try:
        print(f"Realizando requisição para a cidade: {cidade}...")
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            print(f"✓ Dados obtidos com sucesso! (Status: {response.status_code})")
            return response.json()
        else:
            print(f"✗ Erro na requisição: {response.status_code}")
            print(f"Mensagem: {response.json().get('message', 'Erro desconhecido')}")
            return None
            
    except requests.exceptions.Timeout:
        print("✗ Erro: Timeout na requisição")
        return None
    except requests.exceptions.RequestException as e:
        print(f"✗ Erro na requisição: {e}")
        return None


def estruturar_dados(dados_raw):
    """
    Estrutura os dados relevantes da API para inserção no banco
    
    Args:
        dados_raw (dict): Dados brutos da API
        
    Returns:
        dict: Dados estruturados
    """
    return {
        'cidade': dados_raw.get('name'),
        'pais': dados_raw.get('sys', {}).get('country'),
        'temperatura': dados_raw.get('main', {}).get('temp'),
        'sensacao_termica': dados_raw.get('main', {}).get('feels_like'),
        'temp_minima': dados_raw.get('main', {}).get('temp_min'),
        'temp_maxima': dados_raw.get('main', {}).get('temp_max'),
        'pressao': dados_raw.get('main', {}).get('pressure'),
        'umidade': dados_raw.get('main', {}).get('humidity'),
        'descricao': dados_raw.get('weather', [{}])[0].get('description'),
        'velocidade_vento': dados_raw.get('wind', {}).get('speed'),
        'nuvens': dados_raw.get('clouds', {}).get('all'),
        'data_hora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'timestamp_api': dados_raw.get('dt')
    }


def criar_banco_dados():
    """Cria o banco de dados SQLite e a tabela de clima"""
    conn = sqlite3.connect('projeto_rpa.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dados_clima (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cidade TEXT NOT NULL,
            pais TEXT,
            temperatura REAL,
            sensacao_termica REAL,
            temp_minima REAL,
            temp_maxima REAL,
            pressao INTEGER,
            umidade INTEGER,
            descricao TEXT,
            velocidade_vento REAL,
            nuvens INTEGER,
            data_hora TEXT,
            timestamp_api INTEGER
        )
    ''')
    
    conn.commit()
    print("✓ Banco de dados 'projeto_rpa.db' criado/verificado com sucesso!")
    return conn


def inserir_dados_banco(conn, dados):
    """
    Insere os dados estruturados no banco de dados
    
    Args:
        conn: Conexão com o banco de dados
        dados (dict): Dados estruturados para inserção
    """
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO dados_clima (
            cidade, pais, temperatura, sensacao_termica, temp_minima, temp_maxima,
            pressao, umidade, descricao, velocidade_vento, nuvens, data_hora, timestamp_api
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        dados['cidade'],
        dados['pais'],
        dados['temperatura'],
        dados['sensacao_termica'],
        dados['temp_minima'],
        dados['temp_maxima'],
        dados['pressao'],
        dados['umidade'],
        dados['descricao'],
        dados['velocidade_vento'],
        dados['nuvens'],
        dados['data_hora'],
        dados['timestamp_api']
    ))
    
    conn.commit()
    print(f"✓ Dados da cidade '{dados['cidade']}' inseridos no banco com sucesso!")


def exibir_dados_formatados(dados):
    """Exibe os dados de forma formatada no console"""
    print("\n" + "="*60)
    print(f"DADOS METEOROLÓGICOS - {dados['cidade']}, {dados['pais']}")
    print("="*60)
    print(f"Temperatura: {dados['temperatura']}°C")
    print(f"Sensação Térmica: {dados['sensacao_termica']}°C")
    print(f"Mín/Máx: {dados['temp_minima']}°C / {dados['temp_maxima']}°C")
    print(f"Umidade: {dados['umidade']}%")
    print(f"Pressão: {dados['pressao']} hPa")
    print(f"Descrição: {dados['descricao'].capitalize()}")
    print(f"Velocidade do Vento: {dados['velocidade_vento']} m/s")
    print(f"Nebulosidade: {dados['nuvens']}%")
    print(f"Data/Hora da Consulta: {dados['data_hora']}")
    print("="*60 + "\n")


def consultar_dados_banco(conn, limite=5):
    """
    Consulta os últimos registros do banco de dados
    
    Args:
        conn: Conexão com o banco de dados
        limite (int): Número de registros a retornar
    """
    cursor = conn.cursor()
    cursor.execute('''
        SELECT cidade, temperatura, descricao, data_hora 
        FROM dados_clima 
        ORDER BY id DESC 
        LIMIT ?
    ''', (limite,))
    
    registros = cursor.fetchall()
    
    if registros:
        print(f"\n📊 Últimos {len(registros)} registros no banco de dados:")
        print("-" * 70)
        for reg in registros:
            print(f"• {reg[0]} - {reg[1]}°C - {reg[2]} - {reg[3]}")
        print("-" * 70)


def main():
    """Função principal que coordena o fluxo do programa"""
    print("\n🌤️ SISTEMA DE COLETA DE DADOS METEOROLÓGICOS - RPA")
    print("=" * 70)
    
    try:
        # 1. Carregar configurações
        api_key = carregar_configuracoes()
        
        # 2. Criar/conectar ao banco de dados
        conn = criar_banco_dados()
        
        # 3. Lista de cidades para coletar dados
        cidades = ["London", "Sao Paulo", "New York", "Tokyo", "Paris"]
        
        # 4. Coletar dados de cada cidade
        for cidade in cidades:
            print(f"\n--- Processando: {cidade} ---")
            
            # Obter dados da API
            dados_raw = obter_dados_clima(cidade, api_key)
            
            if dados_raw:
                # Estruturar dados
                dados_estruturados = estruturar_dados(dados_raw)
                
                # Exibir dados formatados
                exibir_dados_formatados(dados_estruturados)
                
                # Inserir no banco de dados
                inserir_dados_banco(conn, dados_estruturados)
            
            print()
        
        # 5. Consultar e exibir últimos registros
        consultar_dados_banco(conn, limite=10)
        
        # 6. Fechar conexão
        conn.close()
        print("\n✓ Processo concluído com sucesso!")
        print(f"✓ Banco de dados: projeto_rpa.db")
        
    except ValueError as e:
        print(f"\n✗ Erro de configuração: {e}")
    except sqlite3.Error as e:
        print(f"\n✗ Erro no banco de dados: {e}")
    except Exception as e:
        print(f"\n✗ Erro inesperado: {e}")
    finally:
        print("\n" + "=" * 70)


if __name__ == "__main__":
    main()