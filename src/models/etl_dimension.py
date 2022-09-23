import pandas as pd
from pandas import DataFrame
from datetime import datetime
from utils.helpers import logs
import os
from unidecode import unidecode

class EtlDimension:

    def __init__(self):
        self.__path: str = '/app/data/'

    def _get_files(self) -> tuple:
        '''
        Funcao que extrai os arquivos e converte em dataframes.

        :returns: tupla com os dataframes
        '''

        # importando os dados com o pandas
        df_carros: DataFrame = pd.read_csv(self.__path + 'raw/CargasCarro.csv', sep=';', encoding="ISO-8859-1")
        df_clientes: DataFrame = pd.read_csv(self.__path + 'raw/CargasCliente.csv', sep=';', encoding="ISO-8859-1")
        df_sinistros: DataFrame = pd.read_csv(self.__path + 'raw/CargasSinistro.csv', sep=';', encoding="ISO-8859-1")

        return df_carros, df_clientes, df_sinistros


    def _etl_carros(self, df: DataFrame) -> tuple:
        '''
        Funcao que realiza a etl dos dados de carros e cria as dimensoes do modelo de dados.

        :param df: dataframe com os dados de carros
        :returns: tupla com os dataframes das dimensoes de carros, carros_marcas e carros_modelos
        '''

        if df.shape[0] >= 1:

            # limpando as colunas de texto e deixando em caixa alta
            df['Marca'] = df['Marca'].str.strip().str.upper()
            df['Modelo'] = df['Modelo'].str.strip().str.upper()
            df['Chassi'] = df['Chassi'].str.strip().str.upper()
            df['Placa'] = df['Placa'].str.strip().str.upper()
            df['Cor'] = df['Cor'].str.strip().str.upper()

            # criando a coluna com a data de hoje
            df['data_criacao'] = datetime.now().date()

            # criando a dimensao de carros
            mask: list = ['ID', 'Placa', 'data_criacao']
            dim_carros: DataFrame = df[mask]
            dim_carros = dim_carros.drop_duplicates()
            dim_carros = dim_carros.rename(columns={
                'ID': 'id_carro',
                'Placa': 'placa'
                })

            # criando a dimensao de carros_marcas
            mask: list = ['Marca', 'data_criacao']
            dim_carros_marcas: DataFrame = df[mask]
            dim_carros_marcas = dim_carros_marcas.drop_duplicates()
            dim_carros_marcas['id_carro_marca'] = dim_carros_marcas.index + 1
            dim_carros_marcas = dim_carros_marcas.rename(columns={
                'Marca': 'marca'
                })

            # criando a dimensao de carros_modelos
            mask: list = ['Modelo', 'data_criacao']
            dim_carros_modelos: DataFrame = df[mask]
            dim_carros_modelos = dim_carros_modelos.drop_duplicates()
            dim_carros_modelos['id_carro_modelo'] = dim_carros_modelos.index + 1
            dim_carros_modelos = dim_carros_modelos.rename(columns={
                'Modelo': 'modelo'
                })

            return dim_carros, dim_carros_marcas, dim_carros_modelos

        else:
            raise('Nao ha dados para serem processados.')
            exit()


    def _etl_clientes(self, df: DataFrame) -> DataFrame:
        '''
        Funcao que realiza a etl dos dados de clientes e cria as dimensoes do modelo de dados.

        :param df: dataframe com os dados de clientes
        :returns: dataframe com a dimensao de clientes
        '''
        if df.shape[0] >= 1:
            # filtrando as colunas a serem utilizadas
            mask: list = ['CodCliente', 'Nome', 'CPF']

            # limpando as colunas de texto e deixando em caixa alta
            df['Nome'] = df['Nome'].str.strip().str.upper().str.replace("'", "")

            # convertendo tipos de colunas
            df['CPF'] = df['CPF'].astype(str)

            # criando a coluna com a data de hoje
            df['data_criacao'] = datetime.now().date()

            # limpando acentos dos nomes
            df['Nome'] = df['Nome'].apply(unidecode)

            # criando a dimensao de clientes
            dim_clientes: DataFrame = df.copy()
            mask = ['CodCliente', 'Nome', 'CPF', 'data_criacao']
            dim_clientes = dim_clientes[mask]
            dim_clientes = dim_clientes.drop_duplicates()
            dim_clientes = dim_clientes.rename(columns={
                'CodCliente': 'id_cliente',
                'Nome': 'nome',
                'CPF': 'cpf'
                })

            return dim_clientes

        else:
            raise('Nao ha dados para serem processados.')
            exit()

    
    def _etl_seguros_dimensao(self, df: DataFrame) -> DataFrame:
        '''
        Funcao que realiza a etl dos dados de seguros e cria as dimensoes do modelo de dados.

        :param df: dataframe com os dados de seguros
        :returns: dataframe com a dimensao de cidades
        '''
        if df.shape[0] >= 1:
            # criando a coluna com a data de hoje
            df['data_criacao'] = datetime.now().date()

            # limpando as colunas de texto e deixando em caixa alta
            df['Local Sinistro'] = df['Local Sinistro'].str.strip().str.upper()

            # criando a dimensao de cidades
            mask: list = ['Local Sinistro', 'data_criacao']
            dim_cidades: DataFrame = df[mask]
            dim_cidades = dim_cidades.drop_duplicates()
            dim_cidades['id_cidade'] = dim_cidades.index + 1
            dim_cidades = dim_cidades.rename(columns={
                'Local Sinistro': 'cidade'
                })

            return dim_cidades
        
        else:
            raise('Nao ha dados para serem processados.')
            exit()


    def _etl_calendario(self) -> DataFrame:
        '''
        Funcao que cria a dimensao de calendario.

        :returns: dataframe com a dimensao de calendario
        '''

        # criando a dimensao de calendario
        dim_calendario: DataFrame = pd.DataFrame()
        dim_calendario['data'] = pd.date_range(start='1/1/2000', end='12/31/2030')

        # criando as colunas de ano, mes e dia
        dim_calendario['ano'] = dim_calendario['data'].dt.year
        dim_calendario['mes'] = dim_calendario['data'].dt.month
        dim_calendario['dia'] = dim_calendario['data'].dt.day

        # mapeando os dias da semana
        week_day: list = ['seg', 'ter', 'qua', 'qui', 'sex', 'sab', 'dom']
        dim_calendario["nome_dia_semana"] = dim_calendario['data'].apply(lambda x: week_day[x.weekday()])

        # mapeando os nomes dos meses
        months: list = ["Unknown", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", 
                            "Setembro", "Outubro", "Novembro", "Dezembro"]

        dim_calendario["nome_mes"] = dim_calendario['mes'].apply(lambda x: months[x])

        # criando a coluna de id
        dim_calendario['id_calendario'] = dim_calendario['data'].apply(lambda x: x.strftime('%Y%m%d'))
        dim_calendario['id_calendario'] = dim_calendario['id_calendario'].astype(int)
    
        return dim_calendario


    def etl_dimension(self) -> None:
        '''
        Funcao que executa o ETL para todas as dimensoes do modelo de dados e salva na pasta data.
        '''
        # carregando os dados
        logs('Carregando os dados...')
        df_list: tuple = self._get_files()

        # dim_carros
        logs('Criando a dimensao de carros...')
        df_carros: tuple = self._etl_carros(df_list[0])
        dim_carros: DataFrame = df_carros[0]

        # dim_carros_marcas
        logs('Criando a dimensao de marcas de carros...')
        dim_carros_marcas: DataFrame = df_carros[1]

        # dim_carros_modelos
        logs('Criando a dimensao de modelos de carros...')
        dim_carros_modelos: DataFrame = df_carros[2]

        # dim_clientes
        logs('Criando a dimensao de clientes...')
        dim_clientes: DataFrame = self._etl_clientes(df_list[1])

        # dim_cidades
        logs('Criando a dimensao de cidades...')
        dim_cidades: DataFrame = self._etl_seguros_dimensao(df_list[2])

        # dim_calendario
        logs('Criando a dimensao de calendario...')
        dim_calendario: DataFrame = self._etl_calendario()

        # salvando os dados
        logs('Salvando os dados...')
        os.system(f'rm -rf {self.__path}processed/*.csv')

        try:
            dim_carros.to_csv(self.__path + 'processed/dim_carros.csv', index=False, sep=';')
            dim_carros_marcas.to_csv(self.__path + 'processed/dim_carros_marcas.csv', index=False, sep=';')
            dim_carros_modelos.to_csv(self.__path + 'processed/dim_carros_modelos.csv', index=False, sep=';')
            dim_clientes.to_csv(self.__path + 'processed/dim_clientes.csv', index=False, sep=';')
            dim_cidades.to_csv(self.__path + 'processed/dim_cidades.csv', index=False, sep=';')
            dim_calendario.to_csv(self.__path + 'processed/dim_calendario.csv', index=False, sep=';')

        except Exception as e:
            logs(f'Erro ao salvar os dados: {e}')
            os.system(f'rm -rf {self.__path}processed/*.csv')
            exit()

        logs('ETL das dimensoes finalizado com sucesso!')









    



    

    

