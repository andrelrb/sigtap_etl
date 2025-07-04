ORDEM_PROCESSAMENTO_TABELAS = [
    # Nível 0
    'tb_detalhe', 'tb_ocupacao', 'tb_financiamento', 'tb_habilitacao',
    'tb_tipo_leito', 'tb_grupo', 'tb_registro', 'tb_rede_atencao',
    'tb_renases', 
    # 'tb_sia_sih', # Movida para depois de rl_procedimento_sia_sih
    'tb_regra_condicionada', 'tb_rubrica',
    'tb_cid', 'tb_modalidade', 'tb_grupo_habilitacao', 'tb_servico', 'tb_tuss',

    # Nível 1
    'tb_sub_grupo', 'tb_componente_rede', 'tb_servico_classificacao',
    'tb_descricao_detalhe', 

    # Nível 2
    'tb_forma_organizacao', 

    # Nível 3
    'tb_procedimento', 'tb_descricao', 

    # Nível Intermediário para resolver FK da tb_sia_sih
    'rl_procedimento_sia_sih', # <--- MOVIMENTADA PARA CÁ
    'tb_sia_sih',              # <--- MOVIMENTADA PARA CÁ, DEPOIS DA RL

    # Nível 4: Outras Tabelas 'rl_'
    'rl_procedimento_habilitacao', 'rl_procedimento_renases',
    # 'rl_procedimento_sia_sih', # Já movida
    'rl_procedimento_tuss', 'rl_procedimento_comp_rede',
    'rl_procedimento_cid', 'rl_procedimento_modalidade', 'rl_procedimento_ocupacao',
    'rl_procedimento_servico', 'rl_procedimento_leito', 'rl_procedimento_detalhe',
    'rl_excecao_compatibilidade', 'rl_procedimento_registro',
    'rl_procedimento_compativel', 'rl_procedimento_regra_cond',
    'rl_procedimento_origem', 'rl_procedimento_incremento'
]