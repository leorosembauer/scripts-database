SELECT
    dc.reg_ans,
    COALESCE(op.cnpj, 'NÃO INFORMADO') AS cnpj,
    COALESCE(op.razao_social, 'NÃO INFORMADO') AS razao_social,
    SUM(
        CAST(REPLACE(dc.vl_saldo_final, ',', '.') AS DOUBLE PRECISION) -
        CAST(REPLACE(dc.vl_saldo_inicial, ',', '.') AS DOUBLE PRECISION)
    ) AS despesa
FROM tb_demonstracoes_contabeis dc
LEFT JOIN tb_operadoras op
    ON CAST(TRIM(dc.reg_ans) AS TEXT) = CAST(TRIM(op.Registro_ANS) AS TEXT)
WHERE
    EXTRACT(YEAR FROM dc.data::DATE) = EXTRACT(YEAR FROM CURRENT_DATE) - 1
GROUP BY dc.reg_ans, op.cnpj, op.razao_social
ORDER BY despesa DESC
LIMIT 10;