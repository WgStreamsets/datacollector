CREATE TABLE ALL_TYPES (
  _decimal decimal,
  _tinyint tinyint,
  _smallint smallint,
  _mediumint mediumint,
  _float float,
  _double double,
  _timestamp timestamp,
  _bigint bigint,
  _int int,
  _date date,
  _time time,
  _datetime datetime,
  _year year,
  _varchar varchar(10),
  _enum enum('a', 'b', 'c'),
  _set set('1', '2', '3'),
  _tinyblob tinyblob,
  _mediumblob mediumblob,
  _longblob longblob,
  _blob blob,
  _text text,
  _tinytext tinytext,
  _mediumtext mediumtext,
  _longtext longtext
);

CREATE TABLE foo (
  bar int
);

CREATE TABLE foo2 (
  a int,
  b int,
  c int DEFAULT 3
);
