CREATE TABLE IF NOT EXISTS `member` (
 `id` varchar(64) NOT NULL,
 `source` varchar(32),
 `load_time` varchar(32),
 `indv_id` varchar(64),
 `document` mediumtext NOT NULL,
 PRIMARY KEY (`id`)
);