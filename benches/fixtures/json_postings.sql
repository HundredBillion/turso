CREATE TABLE postings (
    document_id INTEGER NOT NULL,
    tenant INTEGER NOT NULL,
    mode TEXT NOT NULL,
    path TEXT NOT NULL,
    kind TEXT NOT NULL,
    scalar
);
CREATE INDEX postings_lookup ON postings(tenant, mode, path, kind, scalar, document_id);
CREATE INDEX postings_document ON postings(document_id);

CREATE TRIGGER documents_check_insert BEFORE INSERT ON documents BEGIN
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM json_tree(NEW.payload)
        WHERE typeof(key) = 'text' GROUP BY parent, key HAVING count(*) > 1
    ) THEN RAISE(ABORT, 'duplicate JSON keys are unsupported by this prototype') END;
END;
CREATE TRIGGER documents_check_update BEFORE UPDATE ON documents BEGIN
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM json_tree(NEW.payload)
        WHERE typeof(key) = 'text' GROUP BY parent, key HAVING count(*) > 1
    ) THEN RAISE(ABORT, 'duplicate JSON keys are unsupported by this prototype') END;
END;

CREATE TRIGGER documents_postings_insert AFTER INSERT ON documents BEGIN
    INSERT INTO postings
        SELECT NEW.id, NEW.tenant, 'scalar', fullkey,
               CASE type WHEN 'integer' THEN 'number' WHEN 'real' THEN 'number'
                         WHEN 'true' THEN 'boolean' WHEN 'false' THEN 'boolean' ELSE type END,
               atom
        FROM json_tree(NEW.payload) WHERE type NOT IN ('object', 'array');
    INSERT INTO postings
        SELECT NEW.id, NEW.tenant, 'member', path,
               CASE type WHEN 'integer' THEN 'number' WHEN 'real' THEN 'number'
                         WHEN 'true' THEN 'boolean' WHEN 'false' THEN 'boolean' ELSE type END,
               atom
        FROM json_tree(NEW.payload)
        WHERE typeof(key) = 'integer' AND type NOT IN ('object', 'array');
END;

CREATE TRIGGER documents_postings_update AFTER UPDATE ON documents BEGIN
    DELETE FROM postings WHERE document_id = OLD.id;
    INSERT INTO postings
        SELECT NEW.id, NEW.tenant, 'scalar', fullkey,
               CASE type WHEN 'integer' THEN 'number' WHEN 'real' THEN 'number'
                         WHEN 'true' THEN 'boolean' WHEN 'false' THEN 'boolean' ELSE type END,
               atom
        FROM json_tree(NEW.payload) WHERE type NOT IN ('object', 'array');
    INSERT INTO postings
        SELECT NEW.id, NEW.tenant, 'member', path,
               CASE type WHEN 'integer' THEN 'number' WHEN 'real' THEN 'number'
                         WHEN 'true' THEN 'boolean' WHEN 'false' THEN 'boolean' ELSE type END,
               atom
        FROM json_tree(NEW.payload)
        WHERE typeof(key) = 'integer' AND type NOT IN ('object', 'array');
END;

CREATE TRIGGER documents_postings_delete AFTER DELETE ON documents BEGIN
    DELETE FROM postings WHERE document_id = OLD.id;
END;
