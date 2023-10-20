import requests
import csv
import concurrent.futures
from ratelimit import limits, sleep_and_retry
import base64
import psycopg2
import logging as log
from datetime import datetime, timedelta

def ultimo_id():
    conn = psycopg2.connect(
        host="localhost",
        database="banco",
        user="postgres",
        password="senha"
    )
    cur = conn.cursor()
    sql = "select max(ultimo_id) from controle_extracao_hist"
    cur.execute(sql)
    resultados = cur.fetchall()
    cur.close()
    conn.close()
    return resultados[0][0]

def grava_ultimo_id(ticket):
    conn = psycopg2.connect(
        host="localhost",
        database="banco",
        user="postgres",
        password="senha"
    )
    cur = conn.cursor()
    sql = "insert into public.controle_extracao_hist (ultimo_id) values (%s)"
    cur.execute(sql,(ticket,))
    conn.commit()
    cur.close()
    conn.close()
    return 'OK'


# Log
log.basicConfig(filename=f'\\12Log\\p01ExtratorBilhetesGeral_{datetime.now().strftime("%Y%m%d")}.log', level=log.WARNING, format='%(asctime)s - %(message)s')
http_logger = log.getLogger('werkzeug')
http_logger.setLevel(log.ERROR)
log.warning('-----------------------------------------------------------')
log.warning('Iniciando API')
log.warning('-----------------------------------------------------------')
# Variaveis
base_url                = "link fresw=h"
start_id                = ultimo_id()  
end_id                  = start_id+5 
requests_per_minute     = 15
data_hora_atual         = datetime.now().strftime('%Y_%m_%d_%H%M')
nome_arquivo            = "\\11Mov\\tickets_focus_"+data_hora_atual+".csv"
API                     = <"chave da api">
API_base64              = base64.b64encode(API.encode('utf-8')).decode('utf-8')
ultimo_id_processado    = 0
# Main
log.warning('')
log.warning(f'Id inicial: {str(start_id)}')
log.warning(f'Id final: {str(end_id)}')
log.warning(f'Data e hora: {str(data_hora_atual)}')
log.warning('-----------------------------------------------------------')
log.warning('')
headers = {
    "cookie": "_x_w=us; _x_m=x_c",
    "Content-Type": "application/json",        
    "Authorization": f"Basic {API_base64}"                                                                        
}

csv_file = open(nome_arquivo, "w", newline="", encoding="utf-8")
csv_writer = csv.writer(csv_file)
csv_writer.writerow(["Ticket_ID", "ticket_json"])

@sleep_and_retry
@limits(calls=requests_per_minute, period=60)
def process_ticket(ticket_id):
    url = base_url.format(ticket_id)
    response = requests.get(url, headers=headers)
    id_grupo = <id de identificação>
    
    if response.status_code == 200:
        ticket_grupo = response.json()
        group_id = ticket_grupo.get("group_id")     
        if group_id == id_grupo:
            ticket_data = response.json()
            ticket_id = ticket_data.get("id")                
            print(f"Ticket Siga ID {ticket_id} salvo.")
            log.warning(f'Ticket Siga ID {str(ticket_id)} salvo.')
            ultimo_id_processado = ticket_id
            return ticket_id, ticket_data
        else: 
            print(f"Ticket {ticket_id} não é da Siga Antenado")
            log.warning(f'Ticket {str(ticket_id)} não é da Siga Antenado.')
    else:
        print(f"Erro {response.status_code} processando ticket {ticket_id}")
        log.warning(f'Erro  {response.status_code} processando ticket {str(ticket_id)}')

with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    ticket_ids = range(start_id, end_id + 1)
    futures = [executor.submit(process_ticket, ticket_id) for ticket_id in ticket_ids]

    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        if result:
            csv_writer.writerow(result) #+'\n')
            ultimo_id_processado = result[0]

csv_file.close()
log.warning('Utimo Id {ultimo_id_processado}')
grava_ultimo_id(ultimo_id_processado)
print("Fim da busca.")
log.warning(f'API Finalizada')
log.warning('-----------------------------------------------------------')
