create table `travelrecord` (
	`id` bigint (20),
	`user_id` varchar (400),
	`traveldate` date ,
	`fee` Decimal (11),
	`days` int (11),
	`blob` blob ,
	`d` double 
); 
insert into `travelrecord` (`id`, `user_id`, `traveldate`, `fee`, `days`, `blob`, `d`) values('1','1','2019-09-12','222','9','ssss','666.666');
insert into `travelrecord` (`id`, `user_id`, `traveldate`, `fee`, `days`, `blob`, `d`) values('2',NULL,NULL,NULL,NULL,NULL,NULL);
