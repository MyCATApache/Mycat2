create table `COLUMNS` (
	`Field` varchar (256),
	`Type` text ,
	`Null` varchar (12),
	`Key` varchar (12),
	`Default` text ,
	`Extra` varchar (120)
); 
insert into `COLUMNS` (`Field`, `Type`, `Null`, `Key`, `Default`, `Extra`) values('id','int(11)','NO','PRI',NULL,'');
insert into `COLUMNS` (`Field`, `Type`, `Null`, `Key`, `Default`, `Extra`) values('addressname','varchar(20)','YES','',NULL,'');
