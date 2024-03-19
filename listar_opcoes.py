import requests

def listar_opcoes(ativo):
    url = f'https://opcoes.net.br/listaopcoes/completa?au=False&uinhc=0&idLista=ML&idAcao={ativo}&listarVencimentos=true'
    r = requests.get(url).json()
    for linha in r['data']['cotacoesOpcoes']:
            print(linha[0] )
    #l = [[ativo],veni[0].split('_')[0]]
    #return pd.DataFrame(l,columns=['ativo'])

listar_opcoes('PETR4')
