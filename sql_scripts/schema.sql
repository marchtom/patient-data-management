/* Database schema definition */

CREATE TABLE patients (
    id                  SERIAL PRIMARY KEY,
    source_id           TEXT NOT NULL,
    birth_date          DATE,
    gender              TEXT,
    race_code           TEXT,
    race_code_system    TEXT,
    ethnicity_code      TEXT,
    ethnicity_code_system TEXT,
    country             TEXT
);

CREATE TABLE encounters (
    id                  SERIAL PRIMARY KEY,
    source_id           TEXT NOT NULL,
    patient_id          INTEGER NOT NULL,
    start_date          DATE NOT NULL,
    end_date            DATE NOT NULL,
    type_code           TEXT,
    type_code_system    TEXT,
    CONSTRAINT fk_patient FOREIGN KEY (patient_id) REFERENCES patients(id)
);

CREATE TABLE procedures (
    id                  SERIAL PRIMARY KEY,
    source_id           TEXT NOT NULL,
    patient_id          INTEGER NOT NULL,
    encounter_id        INTEGER,
    procedure_date      DATE NOT NULL,
    type_code           TEXT NOT NULL,
    type_code_system    TEXT NOT NULL,
    CONSTRAINT fk_patient FOREIGN KEY (patient_id) REFERENCES patients(id),
    CONSTRAINT fk_encounter FOREIGN KEY (encounter_id) REFERENCES encounters(id)
);

CREATE TABLE observations (
    id                  SERIAL PRIMARY KEY,
    source_id           TEXT NOT NULL,
    patient_id          INTEGER NOT NULL,
    encounter_id        INTEGER,
    observation_date    DATE NOT NULL,
    type_code           TEXT NOT NULL,
    type_code_system    TEXT NOT NULL,
    value               DECIMAL NOT NULL,
    unit_code           TEXT,
    unit_code_system    TEXT,
    CONSTRAINT fk_patient FOREIGN KEY (patient_id) REFERENCES patients(id),
    CONSTRAINT fk_encounter FOREIGN KEY (encounter_id) REFERENCES encounters(id)
);
