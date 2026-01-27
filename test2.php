<?php

declare(strict_types=1);

use Hibla\MysqlClient\Enums\IsolationLevel;
use Hibla\MysqlClient\MysqlConnection;

use function Hibla\async;
use function Hibla\await;

require 'vendor/autoload.php';

async(function () {
    $connection = await(MysqlConnection::create([
        'host' => 'localhost',
        'port' => 3309,
        'password' => 'Reymart1234',
        'database' => 'guitar_lyrics',
    ]));

    // Create a temporary table for testing
    await($connection->query("
        CREATE TEMPORARY TABLE IF NOT EXISTS test_nested (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            level VARCHAR(50)
        )
    "));

    echo "=== Testing Nested Transactions with SAVEPOINTS ===\n\n";

    try {
        // Outer transaction
        echo "Starting outer transaction...\n";
        $transaction = await($connection->beginTransaction(IsolationLevel::READ_COMMITTED));
        
        await($transaction->query("INSERT INTO test_nested (name, level) VALUES ('outer-1', 'outer')"));
        echo "Inserted: outer-1\n";

        try {
            // Create a savepoint for "nested" transaction
            echo "\nCreating savepoint 'sp1'...\n";
            await($transaction->query("SAVEPOINT sp1"));
            
            await($transaction->query("INSERT INTO test_nested (name, level) VALUES ('inner-1', 'inner')"));
            await($transaction->query("INSERT INTO test_nested (name, level) VALUES ('inner-2', 'inner')"));
            echo "Inserted: inner-1, inner-2\n";

            echo "\nData inside savepoint:\n";
            $result = await($transaction->query("SELECT * FROM test_nested"));
            print_r($result->fetchAll());

            // Force error in inner savepoint
            echo "\nForcing error...\n";
            await($transaction->query("INSERT INTO test_nested (name, level) VALUES ('inner-3', REPEAT('X', 100))")); // Too long for VARCHAR(50)

            await($transaction->query("RELEASE SAVEPOINT sp1"));
            
        } catch (\Exception $e) {
            echo "\nInner savepoint error: " . $e->getMessage() . "\n";
            await($transaction->query("ROLLBACK TO SAVEPOINT sp1"));
            echo "Rolled back to savepoint 'sp1'!\n";
        }

        echo "\nData after savepoint rollback:\n";
        $result = await($transaction->query("SELECT * FROM test_nested"));
        print_r($result->fetchAll());

        // Add more data in outer transaction
        await($transaction->query("INSERT INTO test_nested (name, level) VALUES ('outer-2', 'outer')"));
        echo "\nInserted: outer-2\n";

        await($transaction->commit());
        echo "\nOuter transaction committed!\n";
        
    } catch (\Exception $e) {
        echo "\nOuter transaction error: " . $e->getMessage() . "\n";
        if (isset($transaction)) {
            await($transaction->rollback());
            echo "Outer transaction rolled back!\n";
        }
    }

    echo "\n=== Final state ===\n";
    $final = await($connection->query("SELECT * FROM test_nested"));
    print_r($final->fetchAll());
    
})->wait();