SELECT
    reg_ans AS operadora,
    SUM(
        CAST(REPLACE(vl_saldo_inicial, ',', '.') AS DOUBLE PRECISION) -
        CAST(REPLACE(vl_saldo_final, ',', '.') AS DOUBLE PRECISION)
    ) AS total_despesa
FROM tb_demonstracoes_contabeis
WHERE
    descricao LIKE '%EVENTOS/ SINISTROS CONHECIDOS OU AVISADOS%'
    AND descricao LIKE '%MEDICO HOSPITALAR%'
    AND data::DATE >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY reg_ans
ORDER BY total_despesa DESC
LIMIT 10;