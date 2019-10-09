
create table `COLUMNS` (
	`Field` varchar (256),
	`Type` text ,
	`Null` varchar (12),
	`Key` varchar (12),
	`Default` text ,
	`Extra` varchar (120)
); 
insert into `COLUMNS` (`Field`, `Type`, `Null`, `Key`, `Default`, `Extra`) values('id','bigint(20)','NO','',NULL,'');
insert into `COLUMNS` (`Field`, `Type`, `Null`, `Key`, `Default`, `Extra`) values('user_id','varchar(100)','YES','',NULL,'');
insert into `COLUMNS` (`Field`, `Type`, `Null`, `Key`, `Default`, `Extra`) values('traveldate','date','YES','',NULL,'');
insert into `COLUMNS` (`Field`, `Type`, `Null`, `Key`, `Default`, `Extra`) values('fee','decimal(10,0)','YES','',NULL,'');
insert into `COLUMNS` (`Field`, `Type`, `Null`, `Key`, `Default`, `Extra`) values('days','int(11)','YES','',NULL,'');
insert into `COLUMNS` (`Field`, `Type`, `Null`, `Key`, `Default`, `Extra`) values('blob','longblob','YES','',NULL,'');
insert into `COLUMNS` (`Field`, `Type`, `Null`, `Key`, `Default`, `Extra`) values('d','double','YES','',NULL,'');
