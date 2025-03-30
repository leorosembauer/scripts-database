CREATE TABLE IF NOT EXISTS tb_operadoras (
    id SERIAL PRIMARY KEY,
    registro_ans VARCHAR(256),
    cnpj VARCHAR(14),
    razao_social TEXT,
    nome_fantasia TEXT,
    modalidade VARCHAR(50),
    logradouro TEXT,
    numero VARCHAR(10),
    complemento TEXT,
    bairro TEXT,
    cidade TEXT,
    uf CHAR(2),
    cep VARCHAR(10),
    ddd CHAR(256),
    telefone VARCHAR(15),
    fax VARCHAR(15),
    endereco_eletronico TEXT,
    representante TEXT,
    cargo_representante TEXT,
    regiao_de_comercializacao BIGINT,
    data_registro_ans DATE
);


CREATE TABLE IF NOT EXISTS tb_demonstracoes_contabeis (
    id SERIAL PRIMARY KEY,
    data DATE NOT NULL,
    operadora_id INT REFERENCES operadoras(id) ON DELETE CASCADE,
    reg_ans VARCHAR(10) NOT NULL,
    cd_conta_contabil BIGINT NOT NULL,
    descricao TEXT NOT NULL,
    vl_saldo_inicial NUMERIC(15,2),
    vl_saldo_final NUMERIC(15,2),
    data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

