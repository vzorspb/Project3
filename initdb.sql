SELECT 'CREATE DATABASE logdb' WHERE NOT EXISTS (SELECT datname FROM pg_database WHERE datname = 'logdb');

CREATE TABLE public.client_hostname (
	id serial4 NOT NULL,
	ip varchar NULL,
	hostname varchar NULL,
	CONSTRAINT client_hostname_pkey PRIMARY KEY (id)
);

CREATE TABLE public.httpdlog (
	id serial4 NOT NULL,
        "ip" varchar NULL,
	datetime timestamp NULL,
	"method" varchar NULL,
	"path" varchar NULL,
	resultcode varchar NULL,
	"size" varchar NULL,
	referer varchar NULL,
	useragent varchar NULL,
	CONSTRAINT httpdlog_pk PRIMARY KEY (id)
);
