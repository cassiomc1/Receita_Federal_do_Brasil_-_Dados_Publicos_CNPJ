-- Criar a base de dados "Dados_RFB"
CREATE DATABASE "Dados_RFB"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;

COMMENT ON DATABASE "Dados_RFB"
    IS 'Base de dados para gravar os dados públicos de CNPJ da Receita Federal do Brasil';

-- Conectar ao banco "Dados_RFB" antes de executar as tabelas abaixo caso queira criá-las manualmente.
-- O script Python ETL_coletar_dados_e_gravar_BD.py cria as tabelas automaticamente se não existirem.
--
-- ATENÇÃO (divergência de tipos): quando as tabelas são criadas pelo ETL (pandas/to_sql), algumas
-- colunas assumem tipos diferentes deste DDL. Em especial, no schema gerado pelo ETL são INTEGER:
--   "estabelecimento"."cnae_fiscal_principal", "data_inicio_atividade",
--   "data_situacao_cadastral" e "data_situacao_especial"  (aqui declaradas como VARCHAR).
-- Por isso o CNAE perde o zero à esquerda (0111301 -> 111301) e as datas viram números (20200101).
-- A camada web (web/database.py) trata os dois formatos por meio de casts ::text, funcionando
-- corretamente em ambos os schemas.

-- 1. Tabela de Empresas (compatível com CNPJ Alfanumérico - IN RFB 2.229/2024)
CREATE TABLE IF NOT EXISTS "empresa" (
    "cnpj_basico" VARCHAR(8),
    "razao_social" VARCHAR(1000),
    "natureza_juridica" INTEGER,
    "qualificacao_responsavel" INTEGER,
    "capital_social" DOUBLE PRECISION,
    "porte_empresa" INTEGER,
    "ente_federativo_responsavel" VARCHAR(500)
);

-- 2. Tabela de Estabelecimentos
CREATE TABLE IF NOT EXISTS "estabelecimento" (
    "cnpj_basico" VARCHAR(8),
    "cnpj_ordem" VARCHAR(4),
    "cnpj_dv" VARCHAR(2),
    "identificador_matriz_filial" INTEGER,
    "nome_fantasia" VARCHAR(1000),
    "situacao_cadastral" INTEGER,
    "data_situacao_cadastral" VARCHAR(8),
    "motivo_situacao_cadastral" INTEGER,
    "nome_cidade_exterior" VARCHAR(500),
    "pais" INTEGER,
    "data_inicio_atividade" VARCHAR(8),
    "cnae_fiscal_principal" VARCHAR(10),
    "cnae_fiscal_secundaria" TEXT,
    "tipo_logradouro" VARCHAR(100),
    "logradouro" VARCHAR(1000),
    "numero" VARCHAR(100),
    "complemento" VARCHAR(1000),
    "bairro" VARCHAR(500),
    "cep" VARCHAR(20),
    "uf" VARCHAR(10),
    "municipio" INTEGER,
    "ddd_1" VARCHAR(10),
    "telefone_1" VARCHAR(50),
    "ddd_2" VARCHAR(10),
    "telefone_2" VARCHAR(50),
    "ddd_fax" VARCHAR(10),
    "fax" VARCHAR(50),
    "correio_eletronico" VARCHAR(500),
    "situacao_especial" VARCHAR(500),
    "data_situacao_especial" VARCHAR(8)
);

-- 3. Tabela de Sócios
CREATE TABLE IF NOT EXISTS "socios" (
    "cnpj_basico" VARCHAR(8),
    "identificador_socio" INTEGER,
    "nome_socio_razao_social" VARCHAR(1000),
    "cpf_cnpj_socio" VARCHAR(50),
    "qualificacao_socio" INTEGER,
    "data_entrada_sociedade" VARCHAR(8),
    "pais" INTEGER,
    "representante_legal" VARCHAR(50),
    "nome_do_representante" VARCHAR(500),
    "qualificacao_representante_legal" INTEGER,
    "faixa_etaria" INTEGER
);

-- 4. Tabela do Simples Nacional / MEI
CREATE TABLE IF NOT EXISTS "simples" (
    "cnpj_basico" VARCHAR(8),
    "opcao_pelo_simples" VARCHAR(5),
    "data_opcao_simples" VARCHAR(8),
    "data_exclusao_simples" VARCHAR(8),
    "opcao_mei" VARCHAR(5),
    "data_opcao_mei" VARCHAR(8),
    "data_exclusao_mei" VARCHAR(8)
);

-- 5. Tabelas de Domínio
CREATE TABLE IF NOT EXISTS "cnae" (
    "codigo" VARCHAR(20),
    "descricao" TEXT
);

CREATE TABLE IF NOT EXISTS "moti" (
    "codigo" INTEGER,
    "descricao" TEXT
);

CREATE TABLE IF NOT EXISTS "munic" (
    "codigo" INTEGER,
    "descricao" TEXT
);

CREATE TABLE IF NOT EXISTS "natju" (
    "codigo" INTEGER,
    "descricao" TEXT
);

CREATE TABLE IF NOT EXISTS "pais" (
    "codigo" INTEGER,
    "descricao" TEXT
);

CREATE TABLE IF NOT EXISTS "quals" (
    "codigo" INTEGER,
    "descricao" TEXT
);

-- Índices principais (criados após a carga para melhor desempenho):
-- CREATE INDEX IF NOT EXISTS empresa_cnpj ON "empresa"("cnpj_basico");
-- CREATE INDEX IF NOT EXISTS estabelecimento_cnpj ON "estabelecimento"("cnpj_basico");
-- CREATE INDEX IF NOT EXISTS socios_cnpj ON "socios"("cnpj_basico");
-- CREATE INDEX IF NOT EXISTS simples_cnpj ON "simples"("cnpj_basico");
-- Apoia a filtragem e a ordenação por data de criação (início de atividade) na interface web:
-- CREATE INDEX IF NOT EXISTS estabelecimento_data_inicio_atividade ON "estabelecimento"("data_inicio_atividade");