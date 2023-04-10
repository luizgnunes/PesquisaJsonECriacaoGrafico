from google.cloud import bigquery
import plotly.express as px

dataset_id = "salestest"


class DatasetManager(object):

    def __init__(self, dataset_id):
        self.dataset_id = dataset_id
        self.client = self._get_client()

    def print_listed_projeto(self):
        """INFORMA O NOME DO PROJETO DO SERVICE ACCOUNT"""
        projects = list(self.client.list_projects())
        print("Projects:")
        for project in projects:
            print(project.project_id)

    def data_set(self):
        """INFORMA O NOME DO DATASET DO SERVICE ACCOUNT"""
        datasets = list(self.client.list_datasets())
        print("Datasets:")
        for dataset in datasets:
            print(dataset.dataset_id)

    def tabelas(self):
        """INFORMA O NOME DAS TABELAS DO SERVICE ACCOUNT"""
        dataset_id = "sales_test"
        tables = list(self.client.list_tables(dataset_id))
        print("Tables:")
        for table in tables:
            print(table.table_id)

    def _get_client(self):
        return bigquery.Client.from_service_account_json('data/%s.json' % self.dataset_id)

    def query_dataset(self, query):
        return self.client.query(query).result().to_dataframe()

        # DÁ QUERY NA BIG QUERY DO SERVICE ACCOUNT


def get_vendas_por_marca_u6m(dataset_manager):
    query = f"""SELECT 
                brand,
                SUM(value) as vendas_por_marca_u6m
                
                FROM salestest-373621.sales_test.fact_sales_product_day
                LEFT JOIN salestest-373621.sales_test.dim_product
                ON dim_product.product_id = fact_sales_product_day.product_id
                
                WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
                
                GROUP BY brand
                ORDER BY vendas_por_marca_u6m DESC
                """
    df = dataset_manager.query_dataset(query)
    return df.head()


def get_vendas_por_marca_por_dia_u6m(dataset_manager):
    projeto = "salestest-373621.sales_test"

    query = f"""SELECT
                brand,
                date,
                SUM(value) as vendas_por_marca_por_dia_u6m
                
                FROM {projeto}.fact_sales_product_day
                JOIN {projeto}.dim_product
                ON dim_product.product_id = fact_sales_product_day.product_id
                
                WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
                
                GROUP BY brand, date"""
    df = dataset_manager.query_dataset(query)
    return df


def plotar_vendas(dataset_manager):
    query = f"""SELECT 
                brand,
                SUM(value) as vendas_por_marca_u6m
                
                FROM salestest-373621.sales_test.fact_sales_product_day
                LEFT JOIN salestest-373621.sales_test.dim_product
                ON dim_product.product_id = fact_sales_product_day.product_id
                
                WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
                
                GROUP BY brand
                ORDER BY vendas_por_marca_u6m DESC
                """
    df = dataset_manager.query_dataset(query)

    fig = px.bar(df,
                 title="VENDAS",
                 x="brand",
                 y="vendas_por_marca_u6m",
                 color="brand",
                 text_auto='.2s'

                 )
    fig.update_layout(paper_bgcolor="white",
                      plot_bgcolor="white",
                      yaxis_title='Faturamento'
                      )
    fig.update_traces(marker_color='darkgreen',
                      marker_line_color='rgb(8,48,107)',
                      marker_line_width=1.5,
                      opacity=0.9,
                      textfont_size=12,
                      textangle=0,
                      textposition="outside",
                      cliponaxis=False)
    return fig.show()

if __name__ == "__main__":
    query_manager = DatasetManager(dataset_id="salestest")
    plotar_vendas(query_manager)

