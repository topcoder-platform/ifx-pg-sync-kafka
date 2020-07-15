CREATE TABLE
    dynamodb_audit_log
    (
        "SEQ_ID" CHARACTER VARYING(255) NOT NULL,
        "PUBLISH_TIME" TIMESTAMP(6) WITHOUT TIME ZONE,
        PRIMARY KEY ("SEQ_ID")
    );
GRANT ALL ON dynamodb_audit_log TO pgsyncuser;