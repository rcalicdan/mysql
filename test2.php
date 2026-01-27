<?php

declare(strict_types=1);

use Hibla\MysqlClient\MysqlConnection;

require 'vendor/autoload.php';

$connection = MysqlConnection::create([
    'host' => 'localhost',
    'port' => 3309,
    'password' => 'Reymart1234',
    'database' => 'guitar_lyrics',
])->wait();

$results = $connection->query('SELECT * FROM users')->wait();

print_r($results->fetchAll());
