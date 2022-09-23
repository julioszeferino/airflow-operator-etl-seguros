from datetime import datetime
import pandas as pd
from pandas import DataFrame
from utils.helpers import logs
import os

class EtlFact:

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
        df_apolices: DataFrame = pd.read_csv(self.__path + 'raw/CargasApolice.csv', sep=';', encoding="ISO-8859-1")
        dim_carros: DataFrame = pd.read_csv(self.__path + 'processed/dim_carros.csv', sep=';', encoding="ISO-8859-1")
        dim_clientes: DataFrame = pd.read_csv(self.__path + 'processed/dim_clientes.csv', sep=';', encoding="ISO-8859-1")
        dim_carros_marcas: DataFrame = pd.read_csv(self.__path + 'processed/dim_carros_marcas.csv', sep=';', encoding="ISO-8859-1")
        dim_carros_modelos: DataFrame = pd.read_csv(self.__path + 'processed/dim_carros_modelos.csv', sep=';', encoding="ISO-8859-1")
        dim_cidades: DataFrame = pd.read_csv(self.__path + 'processed/dim_cidades.csv', sep=';', encoding="ISO-8859-1")

        return df_carros, df_clientes, df_sinistros, df_apolices, dim_carros, dim_clientes, dim_carros_marcas, dim_carros_modelos, dim_cidades


    def _etl_sinistros(self, df_list: tuple) -> DataFrame:
        '''
        Funcao que realiza o ETL dos dados de sinistros.

        :param df_list: tupla com os dataframes
        :returns: dataframe com a fato sinistros
        '''
        df_carros: DataFrame = df_list[0]
        df_sinistros: DataFrame = df_list[2]
        df_apolices: DataFrame = df_list[3]
        dim_carros_marcas: DataFrame = df_list[6]
        dim_carros_modelos: DataFrame = df_list[7]
        dim_cidades: DataFrame = df_list[8]

        # limpando as colunas de texto e deixando em caixa alta
        df_sinistros['Local Sinistro'] = df_sinistros['Local Sinistro'].str.strip().str.upper()
        
        # busca das informacoes de carros
        mask: list = ['ID', 'Marca', 'Modelo']
        df_carros = df_carros[mask]
        df1: DataFrame = df_sinistros.merge(df_carros, left_on='Carro_CodCarro', right_on='ID', how='left')

        # limpando as colunas de texto e deixando em caixa alta
        df1['Marca'] = df1['Marca'].str.strip().str.upper()
        df1['Modelo'] = df1['Modelo'].str.strip().str.upper()

        # busca da relacao clientes-carros
        mask: list = ['Cliente_CodCliente', 'Carro_CodCarro']
        df_clientes_carros = df_apolices[mask]
        # removendo duplicados
        df_clientes_carros = df_clientes_carros.drop_duplicates()
        # busca das informacoes de clientes
        df2: DataFrame = df1.merge(df_clientes_carros, on='Carro_CodCarro', how='left')

        # busca do id da cidade
        df3: DataFrame = df2.merge(dim_cidades, left_on='Local Sinistro', right_on='cidade', how='left')

        # busca do id da marca
        df4: DataFrame = df3.merge(dim_carros_marcas, left_on='Marca', right_on='marca', how='left')

        # busca do id do modelo
        df5: DataFrame = df4.merge(dim_carros_modelos, left_on='Modelo', right_on='modelo', how='left')

        # Filtrando as colunas
        mask: list = ['CodSinistro', 'DataSinistro', 'id_cidade', 'id_carro_marca', 'id_carro_modelo', 'Cliente_CodCliente', 'Carro_CodCarro']
        df6: DataFrame = df5[mask]

        # criando a sk_data
        df6['DataSinistro'] = pd.to_datetime(df6['DataSinistro'], format='%d/%m/%Y')
        df6['DataSinistro'] = df6['DataSinistro'].apply(lambda x: x.strftime('%Y%m%d'))
        df6['DataSinistro'] = df6['DataSinistro'].astype(int)

        # agrupando as colunas
        df7: DataFrame = df6.groupby(['DataSinistro', 'id_cidade', 'id_carro_marca', 
            'id_carro_modelo', 'Cliente_CodCliente', 'Carro_CodCarro'], as_index=False).agg(
            {'CodSinistro': 'count'})

        # renomeando as colunas
        fato_sinistros: DataFrame = df7.copy()
        
        fato_sinistros = fato_sinistros.rename(columns={
            'DataSinistro': 'sk_data',
            'id_cidade': 'sk_cidade',
            'id_carro_marca': 'sk_carro_marca',
            'id_carro_modelo': 'sk_carro_modelo',
            'Cliente_CodCliente': 'sk_cliente',
            'Carro_CodCarro': 'sk_carro',
            'CodSinistro': 'qtde_sinistros'})

        # criando a coluna de id
        fato_sinistros['id_sinistro'] = fato_sinistros.index + 1


        if fato_sinistros.shape[0] > 0:

            return fato_sinistros

        else:
            raise('Nao ha dados para serem processados.')
            exit()


    def _etl_apolices(self, df_list: tuple) -> DataFrame:
        '''
        Funcao que realiza o ETL dos dados de apolices.

        :param df_list: tupla com os dataframes
        :returns: dataframe com a fato apolices
        '''   
        df_apolices: DataFrame = df_list[3]


        # corrigindo as colunas de data
        df_apolices['DataInicioVigencia'] = pd.to_datetime(df_apolices['DataInicioVigencia'], format='%d/%m/%Y')
        df_apolices['DataFimVigencia'] = pd.to_datetime(df_apolices['DataFimVigencia'], format='%d/%m/%Y')

        # criando a coluna de vigencia
        f = lambda x: 1 if x['DataFimVigencia'] < datetime.now() else 0
        df_apolices['vigencia'] = df_apolices.apply(f, axis=1)

        # agrupando as colunas
        df1: DataFrame = df_apolices.groupby(['Cliente_CodCliente', 'Carro_CodCarro'], as_index=False).agg({
            'CodApolice': 'count',
            'vigencia': 'sum'
        })

        # filtrando as colunas
        fato_apolices: DataFrame = df1.copy()

        fato_apolices = fato_apolices.rename(columns={
            'Cliente_CodCliente': 'sk_cliente',
            'Carro_CodCarro': 'sk_carro',
            'CodApolice': 'qtde_apolices',
            'vigencia': 'qtde_apolices_vigentes'})

        # criando a coluna de id
        fato_apolices['id_apolice'] = fato_apolices.index + 1

        if fato_apolices.shape[0] > 0:
                
                return fato_apolices

        else:
            raise('Nao ha dados para serem processados.')
            exit()
            

    def _etl_fact(self) -> None:
        '''
        Funcao que executa o ETL para todas as fatos do modelo de dados e salva na pasta data.
        '''   
 
        # importando os datasets
        df_all: tuple = self._get_files()

        # criando a fato_sinistros
        fato_sinistros: DataFrame = self._etl_sinistros(df_all)
        
        # criando a fato_apolices
        fato_apolices: DataFrame = self._etl_apolices(df_all)

         # salvando os dados
        logs('Salvando os dados...')

        try:
            fato_sinistros.to_csv(self.__path + 'processed/fato_sinistros.csv', index=False, sep=';')
            fato_apolices.to_csv(self.__path + 'processed/fato_apolices.csv', index=False, sep=';')
            logs('Dados salvos com sucesso!')

        except Exception as e:
            logs(f'Erro ao salvar os dados: {e}')
            os.system(f'rm -rf {self.__path}processed/*.csv')
            exit()

        logs('ETL das dimensoes finalizado com sucesso!')
            