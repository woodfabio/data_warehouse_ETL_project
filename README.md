# Data warehouse ETL project

O objetivo deste projeto é a implementação de uma pequena _data warehouse_ na qual criaremos um processo de ETL em um script Python que extrai dados sobre vendas existentes em arquivos CSV e em um banco de dados MongoDB, subindo-os em um bando de dados PostgreSQL usando modelagem dimensional.
O projeto utiliza Docker Compose para gerar três containers contendo cada um um servidor (MongoDB, Python ou PostgreSQL), os quais estão conectados por uma rede interna.

A seguir serão descritas as etapas presentes deste processo no arquivo "etl.py":

## Parte 1: importações e "delay":
Na primeira parte do código são feitas as importações dos pacotes necessários (os quais estão especificados no arquivo "requirements.txt" de forma a tornar mais práticas as suas importações).
Além disso, foi observado que mesmo com a adição da seguinte especificação na declaração do container _python_app_ no Docker Compose:
```
depends_on:
      - postgres
      - mongodb
```
ainda assim o código de _etl.py_ era executado antes da completa inicialização dos demais containers, razão pela qual usou-se a função "sleep" para gerar um delay na execução do código para dar tempos aos outros containers de iniciarem corretametne.

## Parte 2: configuração de databases:
Em seguida são criadas variáveis que armazenam os valores necessários para efetuar a conexão com os servidores MongoDB e PostreSQL, a saber: _host_ (nome do servidor no Docker Compose), porta, nome do banco de dados, nome de usuário, senha e, no caso do MongoDB, nome da coleção que será lida.

## Parte 3: importação de arquivos CSV:
Nesta parte usamos a função _read_csv_ da biblioteca Pandas para ler os arquivos CSV da pasta _input_ e os salvamos em um dicionário chamado _dfs_ (abreviação de "dataframes"), cuidando para inserir uma nova coluna na tabela _order_payments_ que será usada como chave primária, de acordo com o modelo dimensional desenvolvido.
Após, usamos a função _create_engine_ da biblioteca SQLAlchemy para criar uma conexão com o servidor PostgreSQL (utilizando as variáveis criadas na parte 1) e em seguida usamos a função _to_sql_ da mesma biblioteca para enviar os dataframes do dicionário _dfs_ para o PostgreSQL na forma de tabelas.
Depois, no caso de ocorrer uma exceção ela é descrita no terminal pela  estrutura _try-except-finally_.
Ao final, a estrutura _try-except-finally_ desativa a conexão com o servidor PostgreSQL (caso exista).

## Parte 3: importação do arquivo "init.JS" do MongoDB:
Agora instanciamos um objeto _MongoClient_ da biblioteca pymongo para criar uma conexão com o servidor MongoDB (utilizando as variáveis criadas na parte 1) e acessar a coleção "order_services" que está no arquivo "init.JS". Após, ela é convertida para um dataframe Pandas e novamente usamos as funções _create_engine_ e _to_sql_ da biblioteca SQLAlchemy para enviar este dataframe para o PostgreSQL.
Depois, novamente, no caso de ocorrer uma exceção ela é descrita no terminal pela  estrutura _try-except-finally_ e ao final ela desativa as conexãões com os servidores MongoDB e PostgreSQL (caso existam).

## Parte 4: criando e preenchendo a tabela fato e enviando-a para o PostgreSQL:
Como o projeto tem a exigência de uso de comandos SQL para criação de tabelas agora ao invés de usarmos as funções _create_engine_ e _to_sql_ da biblioteca SQLAlchemy usaremos a bibliotega psycopg2, que aceita comandos SQL e os executa no PostgreSQL. Então primeiramente usamos a função _psycopg2.connect_ (também utilizando as variáveis criadas na parte 1) para criar uma conexão com o PostgreSQL e um objeto _cursor_ que executará as funções que precisaremos. Depois atribuímos comandos SQL de criação e preenchimento da nossa tabela fato _sales_ (seguindo nossa modelagem dimensional) a variáveis que são então passadas como parâmetros para a função _execute_ e em seguida "commitadas" para o PostgreSQL com a função _commit_, de forma a executar estes comandos no banco de dados.
Depois, novamente, no caso de ocorrer uma exceção ela é descrita no terminal pela  estrutura _try-except-finally_.
Ao final, a estrutura _try-except-finally_ desativa a conexão com o servidor PostgreSQL e o objeto _cursor_ (caso existam).
