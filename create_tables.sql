-- Classificação oficial
CREATE TABLE IF NOT EXISTS tabela (
    posicao         INTEGER,
    time            VARCHAR(100) PRIMARY KEY,  
    pontos          INTEGER,
    jogos           INTEGER,
    vitorias        INTEGER,
    empates         INTEGER,
    derrotas        INTEGER,
    gols_pro        INTEGER,
    gols_contra     INTEGER,
    saldo_gols      INTEGER,
    cartoes_amar    INTEGER,
    cartoes_verm    INTEGER,
    aproveitamento  NUMERIC(5,2),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Todas as partidas
CREATE TABLE IF NOT EXISTS partidas (
    ref                 VARCHAR(30) PRIMARY KEY,  -- chave real do pipeline
    rodada              INTEGER,
    data                DATE,                     -- coluna real: "data"
    hora                VARCHAR(10),              -- coluna real: "hora"
    mandante            VARCHAR(100),
    visitante           VARCHAR(100),
    gols_mandante       SMALLINT,
    gols_visitante      SMALLINT,
    estadio             VARCHAR(100),
    cidade              VARCHAR(100),
    uf                  VARCHAR(2)
);

-- Partidas finalizadas (com placar e resultados calculados)
CREATE TABLE IF NOT EXISTS partidas_finalizadas (
    ref                 VARCHAR(30) PRIMARY KEY,
    rodada              INTEGER,
    data                DATE,
    hora                VARCHAR(10),
    mandante            VARCHAR(100),
    visitante           VARCHAR(100),
    gols_mandante       SMALLINT,
    gols_visitante      SMALLINT,
    total_gols          SMALLINT,
    resultado_mandante  VARCHAR(1),
    resultado_visitante VARCHAR(1),
    estadio             VARCHAR(100),
    cidade              VARCHAR(100),
    uf                  VARCHAR(2)
);

-- Estatísticas derivadas por tempo (casa/fora/geral)
CREATE TABLE IF NOT EXISTS team_stats (
    time                   VARCHAR(100) PRIMARY KEY,  
    posicao                INTEGER,
    pontos                 INTEGER,
    aproveitamento         NUMERIC(5,2),
    performance_score      NUMERIC(6,2),
    total_jogos            INTEGER,
    total_vitorias         INTEGER,
    total_empates          INTEGER,
    total_derrotas         INTEGER,
    total_gols_pro         INTEGER,
    total_gols_contra      INTEGER,
    saldo_gols_calc        INTEGER,
    media_gols_pro         NUMERIC(5,2),
    media_gols_contra      NUMERIC(5,2),
    aproveitamento_calc    NUMERIC(5,2),
    jogos_casa             INTEGER,
    vitorias_casa          INTEGER,
    empates_casa           INTEGER,
    derrotas_casa          INTEGER,
    gols_pro_casa          INTEGER,
    gols_contra_casa       INTEGER,
    jogos_fora             INTEGER,
    vitorias_fora          INTEGER,
    empates_fora           INTEGER,
    derrotas_fora          INTEGER,
    gols_pro_fora          INTEGER,
    gols_contra_fora       INTEGER,
    updated_at             TIMESTAMP DEFAULT NOW()
);

-- Forma recente por time
CREATE TABLE IF NOT EXISTS forma_recente (
    time                    VARCHAR(100) PRIMARY KEY,  -- chave real do pipeline
    jogos_recentes          INTEGER,
    vitorias_recentes       INTEGER,
    empates_recentes        INTEGER,
    derrotas_recentes       INTEGER,
    pontos_recentes         INTEGER,
    forma_str               VARCHAR(10),
    aproveitamento_recente  NUMERIC(5,2),
    updated_at              TIMESTAMP DEFAULT NOW()
);

-- Artilharia
CREATE TABLE IF NOT EXISTS artilharia (
    posicao     INTEGER,
    jogador     VARCHAR(100),                   -- chave real do pipeline
    gols        INTEGER,
    clube       VARCHAR(100),                   -- chave real do pipeline
    updated_at  TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (jogador, clube)
);

-- Gols por rodada (tendência)
CREATE TABLE IF NOT EXISTS gols_por_rodada (
    rodada          INTEGER PRIMARY KEY,
    jogos           INTEGER,
    total_gols      INTEGER,
    media_gols      NUMERIC(5,2),
    jogos_com_gols  INTEGER,
    updated_at      TIMESTAMP DEFAULT NOW()
);

---- Índices 
CREATE INDEX IF NOT EXISTS idx_partidas_rodada      ON partidas (rodada);
CREATE INDEX IF NOT EXISTS idx_partidas_mandante    ON partidas (mandante);
CREATE INDEX IF NOT EXISTS idx_partidas_visitante   ON partidas (visitante);
CREATE INDEX IF NOT EXISTS idx_partidas_fin_rodada  ON partidas_finalizadas (rodada);
CREATE INDEX IF NOT EXISTS idx_tabela_posicao       ON tabela (posicao);
CREATE INDEX IF NOT EXISTS idx_artilharia_gols      ON artilharia (gols DESC);
CREATE INDEX IF NOT EXISTS idx_team_stats_posicao   ON team_stats (posicao);


--- corrigindo
DROP TABLE IF EXISTS partidas;
DROP TABLE IF EXISTS partidas_finalizadas;


---
CREATE TABLE IF NOT EXISTS partidas (
    ref                 VARCHAR(30) PRIMARY KEY,
    rodada              INTEGER,
    data                DATE,
    hora                VARCHAR(10),
    mandante            VARCHAR(100),
    visitante           VARCHAR(100),
    gols_mandante       INTEGER,
    gols_visitante      INTEGER,
    estadio             VARCHAR(100),
    cidade              VARCHAR(100),
    uf                  VARCHAR(2)
);


CREATE TABLE IF NOT EXISTS partidas_finalizadas (
    ref                 VARCHAR(30) PRIMARY KEY,
    rodada              INTEGER,
    data                DATE,
    hora                VARCHAR(10),
    mandante            VARCHAR(100),
    visitante           VARCHAR(100),
    gols_mandante       INTEGER,
    gols_visitante      INTEGER,
    total_gols          INTEGER,
    resultado_mandante  VARCHAR(1),
    resultado_visitante VARCHAR(1),
    estadio             VARCHAR(100),
    cidade              VARCHAR(100),
    uf                  VARCHAR(2)
);

ALTER TABLE partidas ALTER COLUMN gols_mandante TYPE INTEGER;
ALTER TABLE partidas ALTER COLUMN gols_visitante TYPE INTEGER;
ALTER TABLE partidas_finalizadas ALTER COLUMN gols_mandante TYPE INTEGER;
ALTER TABLE partidas_finalizadas ALTER COLUMN gols_visitante TYPE INTEGER;
ALTER TABLE partidas_finalizadas ALTER COLUMN total_gols TYPE INTEGER;


SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'partidas'
ORDER BY ordinal_position;