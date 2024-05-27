# Desafio Engenheiro de Dados
Meu desafio dentro desse case era desenvolver um ETL, que tem como objetivo tratar informações de dados do Covid-19 (source ->Johns Hopkins até 2021).

## Resumo dos principais conceitos e technologias utilizadas.
* Pipeline criada na DAG do Airflow
* Airflow rodando localmente dentro de um container (docker)
* Processamento realizado via Spark também dentro do container (docker)
* Formatação especifica na hora de salvar os arquivos parquet, com objetivo de otimização e organização de arquivos, muito importante para FinOps e eficiencia dentro das clouds.
* Boas práticas em desenvolvimento de soluções, o código é mantido em módulos separados e bem documentados para facilitar a leitura e o debug.
* Brinquei um pouco com o conceito de dataquality na dag.
* Analise dos dados, R0 x Média Movel


> 👁️ Viz:
Segue abaixo uma imagem do painel de controle das runs de nossa dag.

![Sucess runs airflow](/_img/runs_airflow.png?raw=true)


#### Estrutura de diretórios e arquivos:

```
desafio-data-engineer/
├── dags/
│   ├── de_challenge_dag.py
│   ├── sample/
│   │   ├── sample.py
│   ├── src/
│   │   ├── resources/
│   │   │   ├── data_quality.py
│   │   │   ├── process_tools.py
│   │   │   ├── spark_module.py
│   │   │   └──  variables.py
│   │   ├── check_modules.py
│   │   ├── r0_modules.py
│   │   ├── refined_modules.py
│   │   └── trusted_module.py
├── datalake/
│   ├── raw/
│   ├── refined/
│   ├── research_r0/
│   └── trusted/
├── .gitignore
├── docker-compose.yaml
├── README.md
├── requirements.txt
├── simple_insights.ipynb
└── trusted_check_notebook.ipynb

```

### Descrição da *DAG*

👁️ Viz:

![dag-de](/_img/dag_de.png?raw=true)

Na DAG acima, adicionei algumas etapas (tasks) adicionais que considero importantes para explorar em uma pipeline. O objetivo é trazer esses conceitos de forma prática e simples, apenas para ilustrar sua aplicação.

Abaixo vou destacar cada um e explicar seu motivo.

👁️ Viz:

![dag-de](/_img/dag_de_checks.png?raw=true)

De maneira resumida, o que trouxe aqui foram conceitos de data quality e a preparação para possíveis mudanças na source. Nas primeiras duas tasks, temos um exemplo de medir alguma métrica da source e, dado seu valor, a branch_task iria seguir o fluxo normal ou entrar em outro fluxo (para fazer algum outro tratamento ou ação, como enviar um e-mail ou mensagem no Discord).

Os outros checks são mais para ilustrar possíveis medições nas camadas trusted e refined. Aqui, fiz muito uso do check_trusted para validar se os dados estavam corretos após o join com a source (utilizei também o notebook trusted_check para visualizar a source).

Por último, adicionei uma task bem ao final, com o objetivo de somar algo (insights) à camada refined. Não coloquei junto na mesma camada para não atrapalhar a correção da task principal do case.

### Insights

Sobre a parte de analise de dados, fiz algo simples dentro do notebook simple_insights, onde busquei expor ambas as medidas calculadas na pipeline em forma de gráfico de linhas e trazer algumas conclusões a partir dos dados.

### Desafios & dificuldades & opinião do desafio


Aqui, creio que a maior dificuldade é ter a vivência de trabalhar com dados pivoteados, algo que não é tão comum quanto o formato tabular. Mesmo já tendo feito a operação de "despivotear", garantir que os dados estão corretos e fazer os checks na source pode ser desafiador, pois é fácil deixar passar algum detalhe despercebido.

No geral achei um desafio bem bacana de executar, gosto muito de aplicar soluções em docker no geral. Deu para ser de certa forma criativo e de adicionar algumas brincadeiras sem ser algo muito massante, acho isso é importante para trazer aprendizados e leveza para dar gosto de fazer as atividades.

>💻 Note: 
Segue aqui também meu estudo de airflow-docker que fiz nesse ano, ficou bem legal também.
https://gabriel-philot.github.io/airflow_studies/ 

Aqui busco explorar alguns conceitos cores do airflow e tentar explicar ele meio que passo a passo em algumas versões.