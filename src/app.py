from models.etl_dimension import EtlDimension
from models.etl_fact import EtlFact
from utils.helpers import logs

def main() -> None:
    '''
    Funcao principal de execucao do projeto.
    '''
    logs('Iniciando o ETL das dimensoes...')
    etl_dim = EtlDimension()
    etl_dim.etl_dimension()

    logs('Iniciando o ETL das fatos...')
    etl_fact = EtlFact()
    etl_fact._etl_fact()

    logs('ELT Concluido...')

if __name__ == '__main__':
    main()