# Dados Públicos CNPJ
- Fonte oficial da Receita Federal do Brasil: [Portal de Dados Abertos](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj) ou repositório direto [arquivos.receitafederal.gov.br](https://arquivos.receitafederal.gov.br/).
- Layout oficial dos arquivos (metadados): [cnpj-metadados.pdf](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf).

A Receita Federal do Brasil disponibiliza mensalmente bases completas com os dados públicos do Cadastro Nacional de Pessoas Jurídicas (CNPJ).

De forma geral, nelas constam as informações do cadastro e cartão CNPJ (matriz e filiais), sócios, opção pelo Simples Nacional e MEI, além de tabelas de domínio auxiliar (CNAEs, Natureza Jurídica, Municípios, etc.).

Neste repositório consta um pipeline de ETL para:
1. **Identificar e baixar** automaticamente os arquivos da base mais recente via WebDAV oficial da Receita Federal (com resumo de downloads interrompidos e barra de progresso);
2. **Descompactar** os arquivos `.zip`;
3. **Ler e tratar** os dados em fluxo contínuo de memória (*chunking*);
4. **Inserir em alta performance** num banco de dados relacional PostgreSQL utilizando o comando nativo `COPY`.

> **Compatibilidade com o CNPJ Alfanumérico:**  
> A estrutura e tipagem dos campos de CNPJ (`cnpj_basico`, `cnpj_ordem`, `cnpj_dv`) foram preparadas para suportar a **Instrução Normativa RFB nº 2.229/2024**, que estabelece o formato alfanumérico para o CNPJ.

---------------------

### Infraestrutura recomendada:
- [Python 3.10+](https://www.python.org/downloads/)
- [PostgreSQL 14+](https://www.postgresql.org/download/) (testado até PostgreSQL 17)
- Espaço em disco recomendado: pelo menos 80 GB livres para armazenamento dos arquivos compactados, descompactados e banco de dados.

---------------------

### Execução Automatizada (Oracle Linux 9 / RHEL 9 / Rocky / AlmaLinux):

Para instalar o PostgreSQL 16, aplicar tuning de performance para ingestão massiva, configurar o ambiente Python e executar todo o processo (download, descompactação e carga no banco) de forma 100% automática:

```bash
# Conceda permissão de execução (se necessário)
chmod +x executar_oracle_linux_9.sh

# Execute com privilégios de superusuário
sudo ./executar_oracle_linux_9.sh
```

> **Dica (Sessões Remotas/SSH):** Como o download e a carga completa podem levar mais de uma hora dependendo da sua conexão e do hardware, recomendamos executar dentro de uma sessão `tmux` ou `screen`:
> ```bash
> tmux new -s rfb
> sudo ./executar_oracle_linux_9.sh
> # Para desconectar: Ctrl+B seguido de D. Para reconectar: tmux attach -t rfb
> ```

Opções adicionais do script:
```bash
# Ver todas as opções
sudo ./executar_oracle_linux_9.sh --help

# Verificar se há nova versão mensal na Receita Federal (em 2 segundos, sem baixar arquivos)
sudo ./executar_oracle_linux_9.sh --check-update

# Atualizar a base para a versão mais recente da RFB (substituição limpa sem duplicação)
sudo ./executar_oracle_linux_9.sh --update

# Customizar senha do banco, diretório de armazenamento e confirmar automaticamente
sudo ./executar_oracle_linux_9.sh --db-password "suasenha" --data-dir "/dados/rfb" -y

# Forçar recarga da mesma versão do zero (mesmo se o banco já estiver atualizado)
sudo ./executar_oracle_linux_9.sh --force --reset
```

> **Atualização Mensal Inteligente (Zero Duplicação):** A Receita Federal publica mensalmente um *snapshot* integral de todas as ~55+ milhões de empresas do país (e não diferenças/deltas). O pipeline agora inclui controle de versão na tabela `_metadados_rfb`:
> - Se o banco já estiver na versão mais recente da RFB, o script avisa imediatamente e não repete o download nem a carga.
> - Ao detectar uma nova versão (ex: `2026-08` -> `2026-09`), os novos arquivos são organizados por mês e a atualização é realizada com substituição limpa, garantindo que o banco permaneça íntegro e **sem nenhum registro duplicado**.

> **Recuperação e Retomada Automática (Checkpointing):** Caso a execução seja interrompida (por queda de conexão, reinicialização ou erro), basta executar o script novamente. Ele identificará o que já foi baixado, descompactado e gravado no banco de dados via tabela de controle `_controle_etl` (indexada por mês e arquivo), continuando exatamente do ponto onde parou sem refazer o trabalho anterior.

---------------------

### Interface Web (Shadcn UI Minimalista):

O repositório inclui uma interface web moderna, rápida e minimalista baseada nos princípios de design do [Shadcn UI](https://ui.shadcn.com/) (tema claro, tipografia Inter, bordas sutis e componentes modulares).

#### Recursos:
- **Autenticação Volátil por Sessão:** A cada inicialização do servidor, um novo par de usuário/senha temporário é gerado aleatoriamente e impresso no terminal. Sessões antigas são invalidadas automaticamente para máxima segurança.
- **Filtros Completos de Consulta:**
  - Identificação: CNPJ (básico ou completo com máscara), Razão Social, Nome Fantasia.
  - Localização: UF e Município (com busca em memória de todos os municípios do Brasil).
  - Situação & Tipo: Situação Cadastral (Ativa, Baixada, Suspensa, Inapta, Nula), Matriz ou Filial.
  - Atividade & Porte: CNAE Principal, Natureza Jurídica, Porte da Empresa (ME, EPP, Demais).
  - Regimes Especiais: Optante pelo Simples Nacional (Sim/Não), Optante pelo MEI (Sim/Não).
  - Financeiro & Datas: Faixa de Capital Social (Mínimo / Máximo) e Data de Início de Atividade.
  - Ordenação: Padrão (CNPJ), Data de Criação (mais recentes / mais antigas), Razão Social (A-Z / Z-A) e Capital Social (maior / menor). A ordenação escolhida é aplicada também às exportações CSV e XLS.
  - Limite de Resultados: campo opcional para trazer apenas os N primeiros registros que atendem aos filtros (ex: 100), acelerando consultas amplas. É respeitado pela paginação e pelas exportações; deixe vazio para sem limite (máximo de 10.000).
- **Consulta sob Demanda (sem carregamento automático):** Ao abrir a página nenhuma consulta de empresas é executada — a tabela exibe o estado "Defina os filtros e clique em Consultar Empresas". Os resultados só são buscados no clique de **Consultar Empresas**; alterar filtros, ordenação ou linhas por página não dispara consultas. Isso evita varrer as tabelas de ~55 milhões de linhas sem necessidade. Apenas os domínios (UFs/Naturezas Jurídicas) e o selo de versão da base são carregados na abertura.
- **Paginação Server-Side:** Paginação otimizada com seletor de itens por página (10, 25, 50, 100) e cálculo total de registros e páginas.
- **Detalhes da Empresa:** Modal completo com abas detalhando Matriz, Endereço e Contato, Enquadramento Tributário, Quadro de Sócios e Administradores (QSA) e Filiais cadastradas.
- **Exportação para CSV e XLS (Excel):** Exportação direta dos resultados filtrados para planilha Excel (.xlsx / .xls) com estilização de cabeçalho, larguras de colunas ajustadas e preservação de formatação de CNPJs e códigos, além de exportação em streaming CSV delimitado por `;` com codificação UTF-8 BOM.
- **Liberação Automática de Firewall (Oracle Linux 9 / RHEL / OCI):** O script verifica se o `firewalld` ou `iptables` está ativo no servidor e adiciona automaticamente a porta utilizada nas regras para liberar o tráfego externo de forma permanente.

#### Como Iniciar:

```bash
# Permissão de execução (se necessário)
chmod +x iniciar_web.sh

# Iniciar na porta padrão (5000)
./iniciar_web.sh

# Ou especificar porta e host:
./iniciar_web.sh --port 8080 --host 0.0.0.0
```

> **Dependências:** o script verifica **módulo a módulo** (e não apenas o Flask) e instala o que estiver faltando no interpretador que executa a aplicação. Se a exportação para Excel exibir o aviso de que `openpyxl` não está instalado, rode no servidor:
> ```bash
> ./venv/bin/pip install -r requirements.txt   # ou: ./venv/bin/pip install openpyxl
> ```
> A exportação em CSV não depende desse pacote e continua funcionando.

Ao iniciar, as credenciais de acesso serão exibidas no terminal:
```text
======================================================================
  CNPJ Explorer - Interface Web (Shadcn UI Minimalista)
======================================================================
  URL de acesso:  http://localhost:5000
  Usuário:        admin
  Senha temporária: rfb-a1b2c3d4e5f6
======================================================================
  [!] As credenciais são voláteis e renovadas a cada inicialização.
======================================================================
```

---------------------

### Como utilizar manualmente (Outros sistemas / Passo a passo):

1. **Instalar dependências:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Preparar o banco de dados:**
   Com o PostgreSQL instalado e rodando, crie a base de dados executando o arquivo `code/banco_de_dados.sql`.

3. **Configurar as variáveis de ambiente:**
   Copie o arquivo `.env_template` no diretório `code` (ou na raiz do repositório) para um arquivo `.env`:
   ```bash
   cp code/.env_template code/.env
   ```
   Edite o `.env` informando os caminhos das pastas e credenciais de acesso ao seu banco de dados:
   - `OUTPUT_FILES_PATH`: diretório para download dos arquivos `.zip`
   - `EXTRACTED_FILES_PATH`: diretório para a extração dos arquivos descompactados
   - `DB_USER`: usuário do PostgreSQL
   - `DB_PASSWORD`: senha do usuário
   - `DB_HOST`: host do banco (ex: `localhost`)
   - `DB_PORT`: porta do banco (ex: `5432`)
   - `DB_NAME`: nome da base de dados (`Dados_RFB`)
   - *(Opcional)* `RFB_ANO_MES`: especifique um mês/ano fixo (ex: `2026-08`). Se omitido, o script detecta e baixa automaticamente a base mais recente disponível.

4. **Executar o processo de ETL:**
   ```bash
   python code/ETL_coletar_dados_e_gravar_BD.py
   ```
   O script também pode ser executado interativamente célula a célula (estilo Jupyter / `# %%`) no VS Code, PyCharm ou Spyder.

---------------------

### Tabelas geradas:
Para maiores detalhes, consulte o documento oficial de [Metadados](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf).
- `empresa`: dados cadastrais da empresa em nível de matriz (razão social, porte, natureza jurídica, capital social, etc.)
- `estabelecimento`: dados por unidade/filial (endereço, telefones, e-mail, CNAE fiscal principal e secundários, situação cadastral, etc.)
- `socios`: dados do quadro de sócios e administradores (QSA)
- `simples`: dados de enquadramento no Simples Nacional e MEI
- `cnae`: código e descrição dos CNAEs
- `quals`: qualificação de sócios, responsável e representante legal
- `natju`: tabela de naturezas jurídicas
- `moti`: motivos da situação cadastral
- `pais`: códigos e nomes de países
- `munic`: códigos e nomes de municípios

As tabelas `empresa`, `estabelecimento`, `socios` e `simples` recebem índices automáticos na coluna `cnpj_basico` ao final da carga para otimizar pesquisas e cruzamentos. A tabela `estabelecimento` também recebe um índice em `data_inicio_atividade`, que acelera a filtragem e a ordenação por data de criação da empresa.

> **Bases já carregadas anteriormente:** como o bloco de criação de índices é ignorado quando já foi executado para a versão atual da base, crie o índice de data manualmente uma única vez (pode levar alguns minutos em ~55 milhões de estabelecimentos):
> ```sql
> CREATE INDEX IF NOT EXISTS estabelecimento_data_inicio_atividade ON "estabelecimento"("data_inicio_atividade");
> ```

### Modelo de Entidade Relacionamento:
![alt text](https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ/blob/master/Dados_RFB_ERD.png)