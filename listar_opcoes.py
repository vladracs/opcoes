import requests

def listar_opcoes(ativo):
    lista_opcoes_vencimentos=[]
    url = f'https://opcoes.net.br/listaopcoes/completa?au=False&uinhc=0&idLista=ML&idAcao={ativo}&listarVencimentos=true'
    r = requests.get(url).json()
    for linha in r['data']['cotacoesOpcoes']:
            name = linha[0]
            lista_opcoes_vencimentos.append(name)
  

    # Print the list of names
    return(lista_opcoes_vencimentos)

print(listar_opcoes('PETR4'))
