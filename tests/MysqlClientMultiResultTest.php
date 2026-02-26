<?php

use function Hibla\await;

beforeAll(function (): void {
    $client = makeClient();

    await($client->query('DROP PROCEDURE IF EXISTS test_multi_results'));

    await($client->query('
            CREATE PROCEDURE test_multi_results(IN p_multiplier INT)
            BEGIN
                SELECT \'First Table\' AS table_name, p_multiplier * 1 AS value;
                SELECT \'Second Table\' AS table_name, p_multiplier * 2 AS value;
                SELECT \'Third Table\' AS table_name, p_multiplier * 3 AS value;
            END
        '));

    $client->close();
});

afterAll(function (): void {
    $client = makeClient();
    await($client->query('DROP PROCEDURE IF EXISTS test_multi_results'));
    $client->close();
});

describe('Stored Procedure Multi Result Sets', function (): void {
    it('returns the first result set via text protocol', function (): void {
        $client = makeClient();

        $result = await($client->query('CALL test_multi_results(10)'));
        $row = $result->fetchOne();

        expect($row['table_name'])->toBe('First Table')
            ->and($row['value'])->toBe('10');

        $client->close();
    });

    it('traverses all three result sets via text protocol', function (): void {
        $client = makeClient();

        $result1 = await($client->query('CALL test_multi_results(10)'));
        $result2 = $result1->nextResult();
        $result3 = $result2?->nextResult();

        expect($result1->fetchOne()['value'])->toBe('10')
            ->and($result2)->not->toBeNull()
            ->and($result2->fetchOne()['value'])->toBe('20')
            ->and($result3)->not->toBeNull()
            ->and($result3->fetchOne()['value'])->toBe('30');

        $client->close();
    });

    it('returns null after the last result set via text protocol', function (): void {
        $client = makeClient();

        $result1 = await($client->query('CALL test_multi_results(10)'));
        $result2 = $result1->nextResult();
        $result3 = $result2?->nextResult();
        $result4 = $result3?->nextResult();

        expect($result4)->toBeNull();

        $client->close();
    });

    it('returns correct table_name values across all result sets via text protocol', function (): void {
        $client = makeClient();

        $result1 = await($client->query('CALL test_multi_results(1)'));
        $result2 = $result1->nextResult();
        $result3 = $result2?->nextResult();

        expect($result1->fetchOne()['table_name'])->toBe('First Table')
            ->and($result2->fetchOne()['table_name'])->toBe('Second Table')
            ->and($result3->fetchOne()['table_name'])->toBe('Third Table');

        $client->close();
    });

    it('traverses all result sets via text protocol using a loop', function (): void {
        $client = makeClient();

        $resultSets = [];
        $current = await($client->query('CALL test_multi_results(10)'));

        do {
            $resultSets[] = $current->fetchOne();
            $current = $current->nextResult();
        } while ($current !== null);

        expect($resultSets)->toHaveCount(3)
            ->and($resultSets[0]['value'])->toBe('10')
            ->and($resultSets[1]['value'])->toBe('20')
            ->and($resultSets[2]['value'])->toBe('30');

        $client->close();
    });

    it('returns the first result set via binary protocol', function (): void {
        $client = makeClient();

        $result = await($client->query('CALL test_multi_results(?)', [10]));
        $row = $result->fetchOne();

        expect($row['table_name'])->toBe('First Table')
            ->and($row['value'])->toBe(10);

        $client->close();
    });

    it('traverses all three result sets via binary protocol', function (): void {
        $client = makeClient();

        $result1 = await($client->query('CALL test_multi_results(?)', [10]));
        $result2 = $result1->nextResult();
        $result3 = $result2?->nextResult();

        expect($result1->fetchOne()['value'])->toBe(10)
            ->and($result2)->not->toBeNull()
            ->and($result2->fetchOne()['value'])->toBe(20)
            ->and($result3)->not->toBeNull()
            ->and($result3->fetchOne()['value'])->toBe(30);

        $client->close();
    });

    it('returns null after the last result set via binary protocol', function (): void {
        $client = makeClient();

        $result1 = await($client->query('CALL test_multi_results(?)', [10]));
        $result2 = $result1->nextResult();
        $result3 = $result2?->nextResult();
        $result4 = $result3?->nextResult();

        expect($result4)->toBeNull();

        $client->close();
    });

    it('returns correct table_name values across all result sets via binary protocol', function (): void {
        $client = makeClient();

        $result1 = await($client->query('CALL test_multi_results(?)', [1]));
        $result2 = $result1->nextResult();
        $result3 = $result2?->nextResult();

        expect($result1->fetchOne()['table_name'])->toBe('First Table')
            ->and($result2->fetchOne()['table_name'])->toBe('Second Table')
            ->and($result3->fetchOne()['table_name'])->toBe('Third Table');

        $client->close();
    });

    it('traverses all result sets via binary protocol using a loop', function (): void {
        $client = makeClient();

        $resultSets = [];
        $current = await($client->query('CALL test_multi_results(?)', [10]));

        do {
            $resultSets[] = $current->fetchOne();
            $current = $current->nextResult();
        } while ($current !== null);

        expect($resultSets)->toHaveCount(3)
            ->and($resultSets[0]['value'])->toBe(10)
            ->and($resultSets[1]['value'])->toBe(20)
            ->and($resultSets[2]['value'])->toBe(30);

        $client->close();
    });

    it('computes correct values for a different multiplier via text protocol', function (): void {
        $client = makeClient();

        $result1 = await($client->query('CALL test_multi_results(5)'));
        $result2 = $result1->nextResult();
        $result3 = $result2?->nextResult();

        expect($result1->fetchOne()['value'])->toBe('5')
            ->and($result2->fetchOne()['value'])->toBe('10')
            ->and($result3->fetchOne()['value'])->toBe('15');

        $client->close();
    });

    it('computes correct values for a different multiplier via binary protocol', function (): void {
        $client = makeClient();

        $result1 = await($client->query('CALL test_multi_results(?)', [5]));
        $result2 = $result1->nextResult();
        $result3 = $result2?->nextResult();

        expect($result1->fetchOne()['value'])->toBe(5)
            ->and($result2->fetchOne()['value'])->toBe(10)
            ->and($result3->fetchOne()['value'])->toBe(15);

        $client->close();
    });

    it('releases connection back to pool after stored procedure call via text protocol', function (): void {
        $client = makeClient(maxConnections: 1);

        await($client->query('CALL test_multi_results(1)'));
        await($client->query('CALL test_multi_results(2)'));

        expect($client->getStats()['pooled_connections'])->toBe(1);

        $client->close();
    });

    it('releases connection back to pool after stored procedure call via binary protocol', function (): void {
        $client = makeClient(maxConnections: 1);

        await($client->query('CALL test_multi_results(?)', [1]));
        await($client->query('CALL test_multi_results(?)', [2]));

        expect($client->getStats()['pooled_connections'])->toBe(1);

        $client->close();
    });

    it('pool remains functional after traversing all result sets', function (): void {
        $client = makeClient(maxConnections: 1);

        $result1 = await($client->query('CALL test_multi_results(?)', [3]));
        $result2 = $result1->nextResult();
        $result3 = $result2?->nextResult();

        $result1->fetchOne();
        $result2?->fetchOne();
        $result3?->fetchOne();

        $result = await($client->query('SELECT ? AS val', [99]));

        expect($result->fetchOne()['val'])->toBe(99);

        $client->close();
    });
});
