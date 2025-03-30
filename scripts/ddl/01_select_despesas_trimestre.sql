-- Dados dos ultimos 3 meses
SELECT
    dc.reg_ans AS operadora,
    COALESCE(op.cnpj, 'NÃO INFORMADO') AS cnpj,
    COALESCE(op.razao_social, 'NÃO INFORMADO') AS razao_social,
    SUM(
        CAST(REPLACE(dc.vl_saldo_inicial, ',', '.') AS DOUBLE PRECISION) -
        CAST(REPLACE(dc.vl_saldo_final, ',', '.') AS DOUBLE PRECISION)
    ) AS total_despesa
FROM tb_demonstracoes_contabeis dc
LEFT JOIN tb_operadoras op
    ON TRIM(dc.reg_ans)::TEXT = TRIM(op.Registro_ANS)::TEXT
WHERE
    dc.descricao LIKE '%EVENTOS/ SINISTROS CONHECIDOS OU AVISADOS%'
    AND dc.descricao LIKE '%MEDICO HOSPITALAR%'
    AND dc.data::DATE >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY dc.reg_ans, op.cnpj, op.razao_social
ORDER BY total_despesa DESC
LIMIT 10;


-- Dados do ultimo trimestre lancado
WITH TrimestreAtual AS (
    SELECT
        EXTRACT(YEAR FROM CURRENT_DATE) AS ano_atual,
        EXTRACT(QUARTER FROM CURRENT_DATE) AS trimestre_atual
),
TrimestreAnterior AS (
    SELECT
        CASE
            WHEN trimestre_atual = 1 THEN ano_atual - 1  -- Se estamos no 1º tri, pegar 4º tri do ano anterior
            ELSE ano_atual  -- Caso contrário, pegar o mesmo ano
        END AS ano_referencia,
        CASE
            WHEN trimestre_atual = 1 THEN 4  -- Se estamos no 1º tri, pegar 4º tri do ano anterior
            ELSE trimestre_atual - 1  -- Caso contrário, pegar o trimestre anterior
        END AS trimestre_referencia
    FROM TrimestreAtual
)
SELECT
    dc.reg_ans AS operadora,
    COALESCE(op.cnpj, 'NÃO INFORMADO') AS cnpj,
    COALESCE(op.razao_social, 'NÃO INFORMADO') AS razao_social,
    SUM(
        CAST(REPLACE(dc.vl_saldo_inicial, ',', '.') AS DOUBLE PRECISION) -
        CAST(REPLACE(dc.vl_saldo_final, ',', '.') AS DOUBLE PRECISION)
    ) AS total_despesa
FROM tb_demonstracoes_contabeis dc
LEFT JOIN tb_operadoras op
    ON TRIM(dc.reg_ans)::TEXT = TRIM(op.Registro_ANS)::TEXT
WHERE
    dc.descricao LIKE '%EVENTOS/ SINISTROS CONHECIDOS OU AVISADOS%'
    AND dc.descricao LIKE '%MEDICO HOSPITALAR%'
    AND EXTRACT(YEAR FROM dc.data::DATE) = (SELECT ano_referencia FROM TrimestreAnterior)
    AND EXTRACT(QUARTER FROM dc.data::DATE) = (SELECT trimestre_referencia FROM TrimestreAnterior)
GROUP BY dc.reg_ans, op.cnpj, op.razao_social
ORDER BY total_despesa DESC
LIMIT 10;
