# MapReduce com Redis - Implementação de Sistema Distribuído

## Visão Geral

Este projeto implementa um sistema MapReduce distribuído utilizando Redis para coordenação de tarefas. O sistema processa grandes volumes de dados dividindo-os em partes menores, processando-as em paralelo e combinando os resultados, permitindo análise eficiente de dados em escala.

## O que é MapReduce?

MapReduce é um modelo de programação para processamento e geração de grandes conjuntos de dados. O modelo consiste em duas operações principais:

- **Map**: Processa pares chave/valor de entrada para gerar pares chave/valor intermediários.
- **Reduce**: Mescla todos os valores intermediários associados à mesma chave.

Este paradigma permite o processamento distribuído de grandes volumes de dados de forma escalável e tolerante a falhas.

## Arquitetura do Sistema

O sistema é composto pelos seguintes componentes:

1. **Coordinator (Coordenador)**: Orquestra todo o processo MapReduce
   - Divide o arquivo de entrada em chunks
   - Gerencia as filas de tarefas
   - Monitora o progresso dos workers
   - Aciona a fase de shuffle
   - Mescla os resultados finais

2. **Mapper Workers**: Processam os chunks de dados
   - Leem os chunks atribuídos
   - Aplicam a função Map (geram pares chave/valor)
   - Escrevem resultados intermediários

3. **Shuffler**: Reorganiza os dados após a fase Map
   - Lê todos os resultados intermediários
   - Agrupa valores pela mesma chave
   - Particiona os dados para os reducers

4. **Reducer Workers**: Agregam os valores por chave
   - Processam os dados agrupados
   - Aplicam a função Reduce
   - Produzem resultados finais

5. **Redis**: Sistema de coordenação centralizada
   - Gerencia filas de tarefas
   - Facilita comunicação entre componentes
   - Mantém contadores e estados

## Fluxo de Execução

1. O arquivo de entrada é dividido em **N** chunks de tamanho aproximadamente igual.
2. Os chunks são colocados em uma fila no Redis.
3. Os Mapper Workers consomem chunks da fila e geram resultados intermediários.
4. Após todos os mappers concluírem, o Shuffler agrupa os dados por chave.
5. Os dados agrupados são divididos em **R** partições para os reducers.
6. Os Reducer Workers processam as partições e geram resultados finais.
7. O Coordinator combina todos os resultados dos reducers.

## Pré-requisitos

- Docker e Docker Compose (para execução com contêineres)
- Ou, para execução local:
  - Python 3.6+
  - Redis Server
  - Pacote Python: `redis`

## Como Executar o Sistema

### Método 1: Usando Docker (Recomendado)

#### Passo 1: Preparação do Ambiente

```bash
git clone https://github.com/flaviotulioalmeida/mapreduce-redis.git
cd mapreduce-redis
```

#### Passo 2: Iniciar o Sistema

```bash
docker compose up --build
```

#### Passo 3: Acompanhar a Execução

```plaintext
coordinator-1  | Splitting input file /app/data/data.txt into 10 chunks
coordinator-1  | Created chunks/chunk0.txt ... chunks/chunk9.txt
coordinator-1  | Pushing mapper tasks to Redis queue
...
coordinator-1  | Final result written to finalresult.txt
```

#### Passo 4: Visualizar os Resultados

```bash
docker compose up -d coordinator
docker compose exec coordinator cat finalresult.txt | head -20
```

#### Passo 5: Copiar os Resultados para o Sistema Local (Opcional)

```bash
CONTAINER_ID=$(docker-compose ps -q coordinator)
docker cp $CONTAINER_ID:/app/finalresult.txt ./finalresult.txt
cat finalresult.txt | head -20
```

#### Passo 6: Encerrar o Sistema

```bash
docker compose down
docker compose down -v
```

### Método 2: Execução Local (Sem Docker)

#### Passo 1: Instalar Dependências

```bash
pip install redis
sudo apt-get install redis-server
sudo systemctl start redis-server
```

#### Passo 2: Gerar Dados de Teste

```bash
mkdir -p data
python data_generator.py --size-mb 100 --output-file data/data.txt
```

#### Passo 3: Iniciar o Coordenador

```bash
python coordinator.py --input-file data/data.txt --num-chunks 10 --num-reducers 5
```

#### Passo 4: Iniciar os Workers

```bash
python run_workers.py --num-mappers 3 --num-reducers 2
```

#### Passo 5: Monitorar o Progresso (Opcional)

```bash
python monitor.py
```

#### Passo 6: Visualizar os Resultados

```bash
head -20 finalresult.txt
```

## Análise dos Resultados

### Formato dos Resultados

```plaintext
palavra  contagem
```

### Interpretação dos Resultados

- Em arquivos com distribuição uniforme, as contagens são similares.
- Em textos reais, a distribuição segue a Lei de Zipf, com poucas palavras muito frequentes e muitas palavras raras.

