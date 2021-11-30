CREATE DATABASE IF NOT EXISTS warehouse;

USE warehouse;

CREATE TABLE IF NOT EXISTS tracks (
    `event_type` int,
    `event_time` DATETIME,
    `data` JSON,
    `processing_date` date
);