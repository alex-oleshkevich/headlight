-- Name: auto
-- Revision: 20220725_190758
-- Author: alex
-- Date: 2022-07-25T19:07:58.158426
-- Transactional: yes

create table if not exists users (
    id integer primary key,
    name text,
    email text,
    password text
);

---- keeps this separator

drop table users;
