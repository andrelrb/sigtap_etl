SELECT
    hc.idhc AS idatendprocessado,
    hc.coturno AS turno,
    hc.counicoregistro AS codigounicoregistro,
    hc.nucpfcnsidadao AS cpfnscidadao,
    hc.nucnsprof AS cnsprofissional,
    hc.cocbo AS cboprofissional,
    hc.nucnesunid AS cnesunidadesaude,
    hc.nuine AS ineunidade,
    hc.cotipoapresentacao AS tipoapresentacaoid,
    hc.cotipoatendimento AS tipoatendidoid,
    hc.cosubtipoatendimento AS cosubtipoatendimento,
    hc.coorigematendimento AS origematendimentoid,
    hc.cotipoconsulta AS tipoconsultaid,
    hc.nucpfestagiario AS cpfestagiario,
    hc.cocboestagiario AS cboestagio,
    hc.dataatendimento AS dataatendimento,
    $1 AS isatendrecente, -- No DBeaver, substitua pelos valores reais
    hdt.CO_ATEND_PROF AS idatendrecente,
    hdt.ST_ATESTADO AS hasatestado,
    hdt.ST_ALERGIA AS hasalergia,
    hdt.ST_IVCF AS hasivcf,
    hdt.ST_MEDICAMENTO AS hasprescricaomedicamento,
    hdt.ST_ORIENTACAO AS hasorientacao,
    hdt.ST_ENCAMINHAMENTO AS hasencaminhamento,
    hdt.ST_PROCED_CLINICO AS hasprocedimentoclinico,
    hdt.ST_MARCADOR_CONSUMO_ALMNTR AS hasmarcadorconsumoalimentar,
    hdt.ST_ANEXO_ARQUIVO AS hasanexoarquivo,
    hdt.CO_CLASSIFICACAO_RISCO AS classificacaoriscoid,
    hdt.TP_ATEND_PROF AS tipoatendprofid,
    hde.ST_SOLICITADO AS hassolicitacaoexame,
    hde.ST_AVALIADO AS hasresultadoexame,
    hdfai.DS_CIAP_NOME_CODIGO AS ciapnomecodigofai,
    hdfai.DS_CID_NOME_CODIGO AS cidnomecodigofai,
    hdfai.ST_FICOU_EM_OBSERVACAO AS hasobservacao,
    hdfai.NO_NOME_FINALIZADOR_OBS AS nomefinalizadorobservacao,
    hdfai.NU_CNS_FINALIZADOR_OBS AS cnsfinalizadorobservacao,
    hdfai.NO_CBO_2002_FINALIZADOR_OBS AS cbofinalizadorobservacao,
    hdfai.NU_CNES_FINALIZADOR_OBS AS ubsfinalizadorobservacao,
    hdfai.NU_INE_FINALIZADOR_OBS AS inefinalizadorobservacao,
    hdfao.DS_CIAP_NOME_CODIGO AS ciapnomecodigofao,
    hdfao.DS_CID_NOME_CODIGO AS cidnomecodigofao,
    hdfad.DS_CIAP_NOME_CODIGO AS ciapnomecodigofad,
    hdfad.DS_CID_NOME_CODIGO AS cidnomecodigofad,
    hdfad.ST_CONDIC_ACAMADO AS acamadofad,
    hdfad.ST_CONDIC_ACOMP_POS_OPERATORIO AS acompanhamentoposoperatoriofad,
    hdfad.ST_CONDIC_ACOMP_PRE_OPERATORIO AS acompanhamentopreoperatoriofad,
    hdfad.ST_CONDIC_ACOMPANHAM_NUTRICION AS acompanhamentonutricionalfad,
    hdfad.ST_CONDIC_ADAPT_USO_ORTES_PROT AS adaptacaousoorteseprotesefad,
    hdfad.ST_CONDIC_CUIDD_PALIAT_N_ONCOL AS cuidadopaliativonaooncologicofad,
    hdfad.ST_CONDIC_CUIDD_PALIAT_ONCOLOG AS cuidadopaliativooncologicofad,
    hdfad.ST_CONDIC_DIALISE_PERITONIAL AS dialiseperitonialfad,
    hdfad.ST_CONDIC_DOMICILIADO AS domiciliadofad,
    hdfad.ST_CONDIC_MEDICACAO_PARENTERAL AS medicacaoparenteralfad,
    hdfad.ST_CONDIC_OXIGENOTERAPIA_DOMIC AS oxigenoterapiadomiciliarfad,
    hdfad.ST_CONDIC_PARACENTESE AS paracentesefad,
    hdfad.ST_CONDIC_REABILITA_DOMICILIAR AS reabilitacaodomiciliarfad,
    hdfad.ST_CONDIC_SUPORT_VENTIL_BIPAP AS suporteventilatoriobipapfad,
    hdfad.ST_CONDIC_SUPORT_VENTIL_CPAP AS suporteventilatoriocpapfad,
    hdfad.ST_CONDIC_ULCERAS_FERIDAS AS ulcerasferidasfad,
    hdfad.ST_CONDIC_USO_ASPIR_VIA_AEREA AS aspiradorviaaereafad,
    hdfad.ST_CONDIC_USO_CISTOSTOMIA AS cistostomiafad,
    hdfad.ST_CONDIC_USO_COLOSTOMIA AS colostomiafad,
    hdfad.ST_CONDIC_USO_GASTROSTOMIA AS gastrotosmiafad,
    hdfad.ST_CONDIC_USO_SOND_VESIC_DEMOR AS sondavesicaldemorafad,
    hdfad.ST_CONDIC_USO_SONDA_NASOENTERA AS sondanasoenteralfad,
    hdfad.ST_CONDIC_USO_SONDA_NASOGASTRI AS sondanasogastricafad,
    hdfad.ST_CONDIC_USO_TRAQUEOSTOMIA AS traqueostomiafad,
    hdv.DS_VACINA_NOME_SIGLA AS vacinanomesigla,
    hdv.ST_GESTANTE AS isgestante,
    hdv.ST_PUERPERA AS ispuerpera,
    hdv.ST_VIAJANTE AS isviajante,
    hdv.ST_COMUNICANTE_HANSENIASE AS iscomunicantehanseniase,
    hdproc.DS_PROCED_NOME_CODIGO AS procedimentonomecodigo,
    hdfae.DS_CID10_PRINCIPAL AS cid10principalfaenomecodigo,
    hdfae.DS_CID10_SECUNDARIO_UM AS cid10secundarioumfaenomecodigo,
    hdfae.DS_CID10_SECUNDARIO_DOIS AS cid10secundariodoisfaenomecodigo,
    hdfae.ST_CONDIC_ACAMADO AS acamadofae,
    hdfae.ST_CONDIC_ACOMP_POS_OPERATORIO AS acompanhamentoposoperatoriofae,
    hdfae.ST_CONDIC_ACOMP_PRE_OPERATORIO AS acompanhamentopreoperatoriofae,
    hdfae.ST_CONDIC_ACOMPANHAM_NUTRICION AS acompanhamentonutricionalfae,
    hdfae.ST_CONDIC_ADAPT_USO_ORTES_PROT AS adaptacaousoorteseprotesefae,
    hdfae.ST_CONDIC_CUIDD_PALIAT_N_ONCOL AS cuidadopaliativonaooncologicofae,
    hdfae.ST_CONDIC_CUIDD_PALIAT_ONCOLOG AS cuidadopaliativooncologicofae,
    hdfae.ST_CONDIC_DIALISE_PERITONIAL AS dialiseperitonialfae,
    hdfae.ST_CONDIC_DOMICILIADO AS domiciliadofae,
    hdfae.ST_CONDIC_MEDICACAO_PARENTERAL AS medicacaoparenteralfae,
    hdfae.ST_CONDIC_OXIGENOTERAPIA_DOMIC AS oxigenoterapiadomiciliarfae,
    hdfae.ST_CONDIC_PARACENTESE AS paracentesefae,
    hdfae.ST_CONDIC_REABILITA_DOMICILIAR AS reabilitacaodomiciliarfae,
    hdfae.ST_CONDIC_SUPORT_VENTIL_BIPAP AS suporteventilatoriobipapfae,
    hdfae.ST_CONDIC_SUPORT_VENTIL_CPAP AS suporteventilatoriocpapfae,
    hdfae.ST_CONDIC_ULCERAS_FERIDAS AS ulcerasferidasfae,
    hdfae.ST_CONDIC_USO_ASPIR_VIA_AEREA AS aspiradorviaaereafae,
    hdfae.ST_CONDIC_USO_CISTOSTOMIA AS cistostomiafae,
    hdfae.ST_CONDIC_USO_COLOSTOMIA AS colostomiafae,
    hdfae.ST_CONDIC_USO_GASTROSTOMIA AS gastrotosmiafae,
    hdfae.ST_CONDIC_USO_SOND_VESIC_DEMOR AS sondavesicaldemorafae,
    hdfae.ST_CONDIC_USO_SONDA_NASOENTERA AS sondanasoenteralfae,
    hdfae.ST_CONDIC_USO_SONDA_NASOGASTRI AS sondanasogastricafae,
    hdfae.ST_CONDIC_USO_TRAQUEOSTOMIA AS traqueostomiafae,
    hdfcc.DS_CIAP_NOME AS ciapnome,
    hdfcc.DS_CIAP_CODIGO AS ciapcodigo,
    hdfcc.DS_CID_NOME AS cidnome,
    hdfcc.DS_CID_CODIGO AS cidcodigo,
    hdfcc.DS_CLASSIFICACAO_PRIORIDADE AS classificacaoprioridade,
    hdfcc.DS_RECLASSIFICACAO_PRIORIDADE AS reclassificacaoprioridade,
    hdfcc.DS_CONDUTA AS conduta,
    hdfcc.NU_CNS_SOLICITANTE AS cnssolicitante,
    hdfcc.NO_NOME_SOLICITANTE AS nomesolicitante,
    hdfcc.NO_CBO_SOLICITANTE AS cbosolicitante,
    hdfcc.NU_INE_SOLICITANTE AS inesolicitante,
    hdfcc.NO_SIGLA_EQUIPE_SOLICITANTE AS siglaequipesolicitante,
    hdfcc.NO_UBS_SOLICITANTE AS nomeubssolicitante,
    hdfcc.NU_CNS_EXECUTANTE AS cnsexecutante,
    hdfcc.NO_NOME_EXECUTANTE AS nomeexecutante,
    hdfcc.NO_CBO_EXECUTANTE AS cboexecutante,
    hdfcc.NU_INE_EXECUTANTE AS ineexecutante,
    hdfcc.NO_SIGLA_EQUIPE_EXECUTANTE AS siglaequipeexecutante,
    hdfcc.NO_UBS_EXECUTANTE AS nomeubsexecutante
FROM
    (
        SELECT
            hcn.CO_SEQ_HISTORICO_CABECALHO AS idhc,
            hcn.DT_ATENDIMENTO AS dataatendimento,
            hcn.CO_TURNO AS coturno,
            hcn.CO_UNICO_REGISTRO AS counicoregistro,
            hcn.NU_CPF_CNS_CIDADAO AS nucpfcnsidadao,
            hcn.NU_CNS_PROF AS nucnsprof,
            hcn.CO_CBO AS cocbo,
            hcn.NU_CNES_UNID AS nucnesunid,
            hcn.NU_INE AS nuine,
            hcn.CO_TIPO_APRESENTACAO AS cotipoapresentacao,
            hcn.CO_TIPO_ATENDIMENTO AS cotipoatendimento,
            hcn.CO_SUBTIPO_ATENDIMENTO AS cosubtipoatendimento,
            hcn.CO_ORIGEM_ATENDIMENTO AS coorigematendimento,
            hcn.CO_TIPO_CONSULTA AS cotipoconsulta,
            hcn.NU_CPF_ESTAGIARIO AS nucpfestagiario,
            hcn.CO_CBO_ESTAGIARIO AS cocboestagiario
        FROM
            tb_historico_cabecalho hcn
        WHERE
            hcn.CO_PRONTUARIO = $2 -- ATENÇÃO: Se CO_PRONTUARIO for numérico, $2 ('390002') precisará de cast: $2::tipo_do_CO_PRONTUARIO
            AND (
                hcn.CO_ORIGEM_ATENDIMENTO = $3 -- Cast similar pode ser necessário aqui e abaixo se as colunas forem numéricas e os parâmetros strings.
                OR hcn.CO_ORIGEM_ATENDIMENTO = $4
            )
            OR hcn.NU_CPF_CNS_CIDADAO IN ($5, $6) -- Se NU_CPF_CNS_CIDADAO for numérico, $5 e $6 precisarão de cast.
            AND (
                hcn.CO_ORIGEM_ATENDIMENTO != $7
                OR hcn.CO_TIPO_APRESENTACAO = $8
            )
    ) hc
LEFT JOIN rl_historico_cabecalho rlhc
    ON hc.idhc = rlhc.CO_FICHA_CONCATENADA::bigint -- CAST ADICIONADO AQUI
LEFT JOIN tb_historico_dados_proced hdproc
    ON hdproc.NU_UUID_FICHA = hc.counicoregistro AND hdproc.NU_CPF_CNS_CIDADAO = hc.nucpfcnsidadao
LEFT JOIN tb_historico_dados_vacina hdv
    ON hdv.NU_UUID_FICHA = hc.counicoregistro AND hdv.NU_CPF_CNS_CIDADAO = hc.nucpfcnsidadao
LEFT JOIN tb_historico_dados_fai hdfai
    ON hdfai.NU_UUID_FICHA = hc.counicoregistro AND hdfai.NU_CPF_CNS_CIDADAO = hc.nucpfcnsidadao
LEFT JOIN tb_historico_dados_fao hdfao
    ON hdfao.NU_UUID_FICHA = hc.counicoregistro AND hdfao.NU_CPF_CNS_CIDADAO = hc.nucpfcnsidadao
LEFT JOIN tb_historico_dados_fad hdfad
    ON hdfad.NU_UUID_FICHA = hc.counicoregistro AND hdfad.NU_CPF_CNS_CIDADAO = hc.nucpfcnsidadao
LEFT JOIN tb_historico_dados_fae hdfae
    ON hdfae.NU_UUID_FICHA = hc.counicoregistro
LEFT JOIN tb_historico_dados_exames hde
    ON hde.NU_UUID_FICHA = hc.counicoregistro AND hde.NU_CPF_CNS_CIDADAO = hc.nucpfcnsidadao
LEFT JOIN tb_historico_dados_fcc hdfcc
    ON hdfcc.NU_UUID_FICHA = hc.counicoregistro AND hdfcc.NU_CPF_CNS_CIDADAO = hc.nucpfcnsidadao
LEFT JOIN tb_historico_dados_tags hdt
    ON hdt.CO_UNICO_REGISTRO = hc.counicoregistro
WHERE
    rlhc.CO_FICHA_ORIGEM_CONCATENACAO IS NULL;