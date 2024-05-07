CREATE DATABASE `development`;

CREATE TABLE `customer` (
  `id` int NOT NULL,
  `firstname` varchar(100) DEFAULT NULL,
  `lastname` varchar(100) DEFAULT NULL,
  `street` varchar(50) DEFAULT NULL,
  `city` varchar(50) DEFAULT NULL
) ;


INSERT INTO `development`.`customer`
(`id`,
`firstname`,
`lastname`,
`street`,
`city`)
VALUES 
(1, "first_name_a", "last_name_a", "street_a", "city_a"), 
(2, "first_name_b", "last_name_b", "street_b", "city_b");
