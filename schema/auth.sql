CREATE ROLE beehive nologin;
GRANT USAGE ON SCHEMA public TO beehive;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO beehive;
GRANT beehive TO sandiego;