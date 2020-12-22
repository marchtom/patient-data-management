INSERT INTO encounters (
    id,
    source_id,
    patient_id,
    start_date,
    end_date
)
VALUES (
    3,
    'encounter-uuid-1',
    7,
    '2020-01-01 00:00:00'::TIMESTAMPTZ,
    '2020-01-02 00:00:00'::TIMESTAMPTZ
);