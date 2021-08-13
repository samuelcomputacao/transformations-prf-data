import os
import psycopg2

def get_ufbr(linha):
    return f'{linha[4]}-{linha[5]}'


def get_km_trunc(linha):
    km = linha[6].split(",")[0]
    if km == 'NA':
        km = '0'
    return f'{km}'


def get_dia_semana_num(linha):
    dias = {'domingo': '0', 'segunda-feira': '1', 'terça-feira': '2', 'quarta-feira': '3', 'quinta-feira': '4',
            'sexta-feira': '5',
            'sábado': '6'}
    return dias[linha[2]]


def get_turno_simples(linha):
    horario = linha[3].split(':')
    hora = int(horario[0])
    return '1' if hora >= 12 else '0'


def get_tipo_pista_simples(linha):
    tipos = {'Múltipla': '1', 'Dupla': '1', 'Simples': '0'}
    return tipos[linha[14]]


def get_categoria_sentido_via(linha):
    tipos = {'Não Informado': '0', 'Crescente': '1', 'Decrescente': '2'}
    return tipos[linha[12]]


def get_tracado_via_simples(linha):
    tipos = {'Não Informado': '0', 'Reta': '1', 'Viaduto': '1', 'Ponte': '1', 'Túnel': '1',
             'Curva': '2', 'Cruzamento': '2', 'Interseção de vias': '2', 'Desvio Temporário': '2', 'Rotatória': '2',
             'Retorno Regulamentado': 2}
    return tipos[linha[15]]


def get_condicao_metereologica_simples(linha):
    tipos = {'Não Informado': '0', 'Ignorado': '0', 'Céu Claro': '1', 'Nublado': '1', 'Sol': '1',
             'Chuva': '2', 'Nevoeiro/Neblina': '2', 'Granizo': '2', 'Vento': '2', 'Garoa/Chuvisco': '2', 'Neve': '2'}
    return tipos[linha[13]]


def get_tipo_acidente_simples(linha):
    tipos = {'Não informado': '0', 'Danos eventuais': '0', 'Incêndio': '0',
             'Eventos atípicos':'0',
             ' Queda de ocupante de veículo': '1',
             'Colisão traseira': '1',
             'Colisão frontal':'1',
             'Colisão transversal':'1',
             'Colisão com objeto estático':'1',
             'Colisão lateral':'1',
             'Colisão com objeto em movimento':'1',
             'Colisão com objeto':'1',
             'Colisão lateral mesmo sentido':'1',
             'Colisão lateral sentido oposto':'1',
             'Engavetamento': '1',
             'Atropelamento de Animal': '1', 'Queda de motocicleta': '1', 'Queda de bicicleta': '1',
             'Queda de ocupante de veículo': '1', 'Atropelamento de Pedestre': '1',
             'Saída de Pista': '2', 'Capotamento': '2', 'Tombamento': '2', 'Derramamento de carga': '2',
             'Saída de leito carroçável':'2'
             }
    return tipos[linha[9]]

def get_classe_gravidade(linha):
    saida = ''
    mortos = int(linha[18])
    feridos_leves = int(linha[19])
    feridos_graves = int(linha[20])
    if not (feridos_leves > 0 or feridos_graves > 0 or mortos > 0):
        saida = 'LEVE'
    elif feridos_leves > 0 and not (feridos_graves > 0 or mortos > 0):
        saida = 'MEDIA'
    elif feridos_graves > 0 and not mortos > 0:
        saida = 'GRAVE'
    elif mortos > 0:
        saida = 'GRAVISSIMO'
    return saida

def get_classe(linha):
    classe = get_classe_gravidade(linha)
    tipos = {'GRAVE':'1', 'GRAVISSIMO':'1','LEVE':'0','MEDIA':'0'}
    return tipos[classe]


def transforma_linha(linha, dados):
    dados['ufbr'] += [get_ufbr(linha)]
    dados['km_trunc'] += [get_km_trunc(linha)]
    dados['dia_semana_num'] += [get_dia_semana_num(linha)]
    dados['turno_simples'] += [get_turno_simples(linha)]
    dados['tipo_pista_simples'] += [get_tipo_pista_simples(linha)]
    dados['categoria_sentido_via'] += [get_categoria_sentido_via(linha)]
    dados['tracado_via_simples'] += [get_tracado_via_simples(linha)]
    dados['condicao_metereologica_simples'] += [get_condicao_metereologica_simples(linha)]
    dados['tipo_acidente_simples'] += [get_tipo_acidente_simples(linha)]
    dados['classe_gravidade'] += [get_classe_gravidade(linha)]
    dados['classe'] += get_classe(linha)


pasta = 'dados'
caminhos = [os.path.join(pasta, nome) for nome in os.listdir(pasta)]
arquivos = [arq for arq in caminhos if os.path.isfile(arq)]
paths = [arq for arq in arquivos if arq.lower().endswith(".csv")]

dados = {'ufbr': [], 'km_trunc': [], 'dia_semana_num': [], 'turno_simples': [], 'tipo_pista_simples': [],
         'categoria_sentido_via': [], 'tracado_via_simples': [], 'condicao_metereologica_simples': [],
         'tipo_acidente_simples': [], 'classe_gravidade': [], 'classe': []}

cont = 0
for path in paths:
    file = open(path, mode='r', encoding="ISO-8859-1")
    flag = True
    for line in file:
        headers = list(map(lambda x: x.strip().replace('"', ''), line.split(";")))
        if flag:
            flag = False
        else:
            line = list(map(lambda x: x.strip().replace('"', ''), line.split(";")))
            transforma_linha(line, dados)
            cont += 1
    file.close()

connection = psycopg2.connect(host='localhost', database='prf', user='postgres', password='12345')
cur = connection.cursor()
sql = "CREATE TABLE IF NOT EXISTS dados_prf(" \
      "ufbr VARCHAR(10)," \
      "km_trunc INTEGER," \
      "dia_semana_num INTEGER," \
      "turno_simples INTEGER," \
      "tipo_pista_simples INTEGER," \
      "categoria_sentido_via INTEGER," \
      "tracado_via_simples INTEGER," \
      "condicao_metereologica_simples INTEGER," \
      "tipo_acidente_simples INTEGER," \
      "classe_gravidade VARCHAR(15)," \
      "classe INTEGER)"
cur.execute(sql)
connection.commit()
for i in range(cont):
    sql = f"INSERT INTO dados_prf (ufbr,km_trunc,dia_semana_num,turno_simples,tipo_pista_simples,categoria_sentido_via,tracado_via_simples," \
          f"condicao_metereologica_simples,tipo_acidente_simples,classe_gravidade,classe)" \
          f" VALUES ('{dados['ufbr'][i]}',{dados['km_trunc'][i]},{dados['dia_semana_num'][i]},{dados['turno_simples'][i]}," \
          f"{dados['tipo_pista_simples'][i]},{dados['categoria_sentido_via'][i]},{dados['tracado_via_simples'][i]}," \
          f"{dados['condicao_metereologica_simples'][i]},{dados['tipo_acidente_simples'][i]},'{dados['classe_gravidade'][i]}',{dados['classe'][i]})"
    # try:
    cur.execute(sql)
    print(sql)
    # except :
    #     print(e)
    #     print(sql)

connection.commit()
connection.close()
