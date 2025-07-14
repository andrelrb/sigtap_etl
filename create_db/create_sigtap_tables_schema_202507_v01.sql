
-- Script SQL gerado para o banco SIGTAP
-- Arquivo: create_sigtap_tables_schema_202507_v01.sql
-- Data de geração: 2025-07-14 11:19:14

CREATE TABLE IF NOT EXISTS public."tb_version" (
    id BIGSERIAL PRIMARY KEY,
    competencia VARCHAR(6) NOT NULL, -- UNIQUE foi REMOVIDO daqui
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE public."tb_version" IS 'Tabela para versionamento. Permite múltiplas entradas para a mesma competência.';



-- Criação da tabela "public.rl_excecao_compatibilidade"
CREATE TABLE IF NOT EXISTS public."rl_excecao_compatibilidade" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento_restricao" VARCHAR(10),
    "co_procedimento_principal" VARCHAR(10),
    "co_registro_principal" VARCHAR(2),
    "co_procedimento_compativel" VARCHAR(10),
    "co_registro_compativel" VARCHAR(2),
    "tp_compatibilidade" VARCHAR(1),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_excecao_compatibilidade_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.rl_procedimento_cid"
CREATE TABLE IF NOT EXISTS public."rl_procedimento_cid" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "co_cid" VARCHAR(4),
    "st_principal" CHAR(1),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_procedimento_cid_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.rl_procedimento_comp_rede"
CREATE TABLE IF NOT EXISTS public."rl_procedimento_comp_rede" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "co_componente_rede" VARCHAR(10),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_procedimento_comp_rede_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.rl_procedimento_compativel"
CREATE TABLE IF NOT EXISTS public."rl_procedimento_compativel" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento_principal" VARCHAR(10),
    "co_registro_principal" VARCHAR(2),
    "co_procedimento_compativel" VARCHAR(10),
    "co_registro_compativel" VARCHAR(2),
    "tp_compatibilidade" VARCHAR(1),
    "qt_permitida" NUMERIC,
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_procedimento_compativel_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.rl_procedimento_detalhe"
CREATE TABLE IF NOT EXISTS public."rl_procedimento_detalhe" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "co_detalhe" VARCHAR(3),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_procedimento_detalhe_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.rl_procedimento_habilitacao"
CREATE TABLE IF NOT EXISTS public."rl_procedimento_habilitacao" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "co_habilitacao" VARCHAR(4),
    "nu_grupo_habilitacao" VARCHAR(4),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_procedimento_habilitacao_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.rl_procedimento_incremento"
CREATE TABLE IF NOT EXISTS public."rl_procedimento_incremento" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "co_habilitacao" VARCHAR(4),
    "vl_percentual_sh" NUMERIC,
    "vl_percentual_sa" NUMERIC,
    "vl_percentual_sp" NUMERIC,
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_procedimento_incremento_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.rl_procedimento_leito"
CREATE TABLE IF NOT EXISTS public."rl_procedimento_leito" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "co_tipo_leito" VARCHAR(2),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_procedimento_leito_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.rl_procedimento_modalidade"
CREATE TABLE IF NOT EXISTS public."rl_procedimento_modalidade" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "co_modalidade" VARCHAR(2),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_procedimento_modalidade_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.rl_procedimento_ocupacao"
CREATE TABLE IF NOT EXISTS public."rl_procedimento_ocupacao" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "co_ocupacao" CHAR(6),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_procedimento_ocupacao_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.rl_procedimento_origem"
CREATE TABLE IF NOT EXISTS public."rl_procedimento_origem" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "co_procedimento_origem" VARCHAR(10),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_procedimento_origem_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.rl_procedimento_registro"
CREATE TABLE IF NOT EXISTS public."rl_procedimento_registro" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "co_registro" VARCHAR(2),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_procedimento_registro_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.rl_procedimento_regra_cond"
CREATE TABLE IF NOT EXISTS public."rl_procedimento_regra_cond" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "co_regra_condicionada" VARCHAR(4),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_procedimento_regra_cond_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.rl_procedimento_renases"
CREATE TABLE IF NOT EXISTS public."rl_procedimento_renases" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "co_renases" VARCHAR(10),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_procedimento_renases_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.rl_procedimento_servico"
CREATE TABLE IF NOT EXISTS public."rl_procedimento_servico" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "co_servico" VARCHAR(3),
    "co_classificacao" VARCHAR(3),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_procedimento_servico_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.rl_procedimento_sia_sih"
CREATE TABLE IF NOT EXISTS public."rl_procedimento_sia_sih" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "co_procedimento_sia_sih" VARCHAR(10),
    "tp_procedimento" VARCHAR(1),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_procedimento_sia_sih_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.rl_procedimento_tuss"
CREATE TABLE IF NOT EXISTS public."rl_procedimento_tuss" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "co_tuss" VARCHAR(10),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_rl_procedimento_tuss_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_cid"
CREATE TABLE IF NOT EXISTS public."tb_cid" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_cid" VARCHAR(4),
    "no_cid" VARCHAR(100),
    "tp_agravo" CHAR(1),
    "tp_sexo" CHAR(1),
    "tp_estadio" CHAR(1),
    "vl_campos_irradiados" NUMERIC,
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT "uq_tb_cid_cid_v" UNIQUE ("co_cid", "version_id"),
    CONSTRAINT fk_tb_cid_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_componente_rede"
CREATE TABLE IF NOT EXISTS public."tb_componente_rede" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_componente_rede" VARCHAR(10),
    "no_componente_rede" VARCHAR(150),
    "co_rede_atencao" VARCHAR(3),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_tb_componente_rede_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_descricao_detalhe"
CREATE TABLE IF NOT EXISTS public."tb_descricao_detalhe" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_detalhe" VARCHAR(3),
    "ds_detalhe" VARCHAR(4000),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_tb_descricao_detalhe_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_descricao"
CREATE TABLE IF NOT EXISTS public."tb_descricao" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "ds_procedimento" VARCHAR(4000),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_tb_descricao_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_detalhe"
CREATE TABLE IF NOT EXISTS public."tb_detalhe" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_detalhe" VARCHAR(3),
    "no_detalhe" VARCHAR(100),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT "uq_tb_detalhe_detalhe_v" UNIQUE ("co_detalhe", "version_id"),
    CONSTRAINT fk_tb_detalhe_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_financiamento"
CREATE TABLE IF NOT EXISTS public."tb_financiamento" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_financiamento" VARCHAR(2),
    "no_financiamento" VARCHAR(100),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT "uq_tb_financiamento_financiamento_v" UNIQUE ("co_financiamento", "version_id"),
    CONSTRAINT fk_tb_financiamento_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_forma_organizacao"
CREATE TABLE IF NOT EXISTS public."tb_forma_organizacao" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_grupo" VARCHAR(2),
    "co_sub_grupo" VARCHAR(2),
    "co_forma_organizacao" VARCHAR(2),
    "no_forma_organizacao" VARCHAR(100),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_tb_forma_organizacao_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_grupo_habilitacao"
CREATE TABLE IF NOT EXISTS public."tb_grupo_habilitacao" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "nu_grupo_habilitacao" VARCHAR(4),
    "no_grupo_habilitacao" VARCHAR(20),
    "ds_grupo_habilitacao" VARCHAR(250),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_tb_grupo_habilitacao_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_grupo"
CREATE TABLE IF NOT EXISTS public."tb_grupo" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_grupo" VARCHAR(2),
    "no_grupo" VARCHAR(100),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT "uq_tb_grupo_grupo_v" UNIQUE ("co_grupo", "version_id"),
    CONSTRAINT fk_tb_grupo_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_habilitacao"
CREATE TABLE IF NOT EXISTS public."tb_habilitacao" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_habilitacao" VARCHAR(4),
    "no_habilitacao" VARCHAR(150),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT "uq_tb_habilitacao_habilitacao_v" UNIQUE ("co_habilitacao", "version_id"),
    CONSTRAINT fk_tb_habilitacao_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_modalidade"
CREATE TABLE IF NOT EXISTS public."tb_modalidade" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_modalidade" VARCHAR(2),
    "no_modalidade" VARCHAR(100),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT "uq_tb_modalidade_modalidade_v" UNIQUE ("co_modalidade", "version_id"),
    CONSTRAINT fk_tb_modalidade_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_ocupacao"
CREATE TABLE IF NOT EXISTS public."tb_ocupacao" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_ocupacao" CHAR(6),
    "no_ocupacao" VARCHAR(150),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT "uq_tb_ocupacao_ocupacao_v" UNIQUE ("co_ocupacao", "version_id"),
    CONSTRAINT fk_tb_ocupacao_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_procedimento"
CREATE TABLE IF NOT EXISTS public."tb_procedimento" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento" VARCHAR(10),
    "no_procedimento" VARCHAR(250),
    "tp_complexidade" VARCHAR(1),
    "tp_sexo" VARCHAR(1),
    "qt_maxima_execucao" NUMERIC,
    "qt_dias_permanencia" NUMERIC,
    "qt_pontos" NUMERIC,
    "vl_idade_minima" NUMERIC,
    "vl_idade_maxima" NUMERIC,
    "vl_sh" NUMERIC,
    "vl_sa" NUMERIC,
    "vl_sp" NUMERIC,
    "co_financiamento" VARCHAR(2),
    "co_rubrica" VARCHAR(6),
    "qt_tempo_permanencia" NUMERIC,
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT "uq_tb_procedimento_procedimento_v" UNIQUE ("co_procedimento", "version_id"),
    CONSTRAINT fk_tb_procedimento_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_rede_atencao"
CREATE TABLE IF NOT EXISTS public."tb_rede_atencao" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_rede_atencao" VARCHAR(3),
    "no_rede_atencao" VARCHAR(50),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT "uq_tb_rede_atencao_rede_atencao_v" UNIQUE ("co_rede_atencao", "version_id"),
    CONSTRAINT fk_tb_rede_atencao_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_registro"
CREATE TABLE IF NOT EXISTS public."tb_registro" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_registro" VARCHAR(2),
    "no_registro" VARCHAR(50),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT "uq_tb_registro_registro_v" UNIQUE ("co_registro", "version_id"),
    CONSTRAINT fk_tb_registro_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_regra_condicionada"
CREATE TABLE IF NOT EXISTS public."tb_regra_condicionada" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_regra_condicionada" VARCHAR(4),
    "no_regra_condicionada" VARCHAR(150),
    "ds_regra_condicionada" VARCHAR(4000),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT "uq_tb_regra_condicionada_regra_condicionada_v" UNIQUE ("co_regra_condicionada", "version_id"),
    CONSTRAINT fk_tb_regra_condicionada_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_renases"
CREATE TABLE IF NOT EXISTS public."tb_renases" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_renases" VARCHAR(10),
    "no_renases" VARCHAR(150),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT "uq_tb_renases_renases_v" UNIQUE ("co_renases", "version_id"),
    CONSTRAINT fk_tb_renases_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_rubrica"
CREATE TABLE IF NOT EXISTS public."tb_rubrica" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_rubrica" VARCHAR(6),
    "no_rubrica" VARCHAR(100),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT "uq_tb_rubrica_rubrica_v" UNIQUE ("co_rubrica", "version_id"),
    CONSTRAINT fk_tb_rubrica_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_servico_classificacao"
CREATE TABLE IF NOT EXISTS public."tb_servico_classificacao" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_servico" VARCHAR(3),
    "co_classificacao" VARCHAR(3),
    "no_classificacao" VARCHAR(150),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_tb_servico_classificacao_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_servico"
CREATE TABLE IF NOT EXISTS public."tb_servico" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_servico" VARCHAR(3),
    "no_servico" VARCHAR(120),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_tb_servico_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_sia_sih"
CREATE TABLE IF NOT EXISTS public."tb_sia_sih" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_procedimento_sia_sih" VARCHAR(10),
    "no_procedimento_sia_sih" VARCHAR(100),
    "tp_procedimento" VARCHAR(1),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_tb_sia_sih_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_sub_grupo"
CREATE TABLE IF NOT EXISTS public."tb_sub_grupo" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_grupo" VARCHAR(2),
    "co_sub_grupo" VARCHAR(2),
    "no_sub_grupo" VARCHAR(100),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT fk_tb_sub_grupo_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_tipo_leito"
CREATE TABLE IF NOT EXISTS public."tb_tipo_leito" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_tipo_leito" VARCHAR(2),
    "no_tipo_leito" VARCHAR(60),
    "dt_competencia" CHAR(6),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT "uq_tb_tipo_leito_tipo_leito_v" UNIQUE ("co_tipo_leito", "version_id"),
    CONSTRAINT fk_tb_tipo_leito_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);


-- Criação da tabela "public.tb_tuss"
CREATE TABLE IF NOT EXISTS public."tb_tuss" (
    id BIGSERIAL NOT NULL,
    "version_id" BIGINT NOT NULL,
    "co_tuss" VARCHAR(10),
    "no_tuss" VARCHAR(450),
    PRIMARY KEY (id, "version_id"),
    CONSTRAINT "uq_tb_tuss_tuss_v" UNIQUE ("co_tuss", "version_id"),
    CONSTRAINT fk_tb_tuss_version_id FOREIGN KEY ("version_id") REFERENCES public."tb_version" (id)
);