<?php
declare(strict_types=1);
function check(bool $condition, string $message): void {
    if (!$condition) { throw new RuntimeException($message); }
}
function openDb(string $path): PDO {
    return new PDO('sqlite:'.$path, null, null, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_TIMEOUT => 0]);
}
$engine = $argv[1];
$path = $argv[2];
$memory = openDb(':memory:');
if ($engine === 'turso') {
    check(is_string($memory->query('SELECT turso_version()')->fetchColumn()), 'Turso identity');
    $maps = file_get_contents('/proc/self/maps');
    check(str_contains($maps, 'libturso_sqlite3.so'), 'fresh Turso library mapped');
    check(!str_contains($maps, '/libsqlite3.so'), 'unexpected stock SQLite mapping');
}
$memory = null;
$before = hash_file('sha256', $path);
$ro = openDb('file:'.$path.'?mode=ro');
check((int)$ro->query('SELECT count(*) FROM sample')->fetchColumn() === 1, 'readonly read');
$ro->exec('CREATE TEMP TABLE scratch(x)');
$ro->exec('INSERT INTO scratch VALUES(7)');
check((int)$ro->query('SELECT x FROM scratch')->fetchColumn() === 7, 'writable TEMP');
try {
    $ro->exec('INSERT INTO sample VALUES(99)');
    throw new RuntimeException('readonly main accepted write');
} catch (PDOException $e) {
    check((int)$e->errorInfo[1] === 8, 'readonly error code');
}
check(hash_file('sha256', $path) === $before, 'readonly operations changed main file');
$writer = openDb($path);
$writer->exec('INSERT INTO sample VALUES(2)');
check((int)$writer->query('SELECT count(*) FROM sample')->fetchColumn() === 2, 'writer after readonly');
$writer = null;
$ro = null;
$a = openDb(':memory:');
$b = openDb(':memory:');
$a->sqliteCreateFunction('connection_value', fn() => 111, 0);
$b->sqliteCreateFunction('connection_value', fn() => 222, 0);
check((int)$a->query('SELECT connection_value()')->fetchColumn() === 111, 'callback isolation a');
check((int)$b->query('SELECT connection_value()')->fetchColumn() === 222, 'callback isolation b');
for ($i = 0; $i < 64; $i++) {
    check($a->sqliteCreateFunction('registered_'.$i, fn() => 42, 0), 'function registration');
    check((int)$a->query('SELECT registered_'.$i.'()')->fetchColumn() === 42, 'function execution');
}
try {
    openDb(dirname($path).'/missing-parent/database.db');
    throw new RuntimeException('missing parent opened');
} catch (PDOException $e) {
    check((int)$e->errorInfo[1] === 14, 'failed-open error code');
}
function storageBytes(string $path): array {
    $result = [];
    foreach (glob($path.'*') as $file) { $result[basename($file)] = hash_file('sha256', $file); }
    ksort($result);
    return $result;
}
foreach (['legacy', 'wal', 'mvcc'] as $mode) {
    if ($mode === 'mvcc' && $engine !== 'turso') { continue; }
    foreach (['reader-first', 'writer-first'] as $closeOrder) {
        $file = $path.'.'.$mode.'.'.$closeOrder.'.db';
        if ($mode === 'legacy') { copy($argv[3], $file); }
        $setup = $mode === 'legacy' ? null : openDb($file);
        if ($mode !== 'legacy') {
            check(strtolower((string)$setup->query('PRAGMA journal_mode='.$mode)->fetchColumn()) === $mode, 'fixture journal mode');
        }
        if ($setup !== null) {
            $setup->exec('CREATE TABLE sample(x)');
            $setup->exec('INSERT INTO sample VALUES(1)');
        }
        $setup = null;
        $bytes = storageBytes($file);
        $reader = openDb('file:'.$file.'?mode=ro');
        $pending = openDb($file);
        $pending->exec('CREATE TEMP TABLE private_temp(x)');
        $pending->exec('INSERT INTO private_temp VALUES(1)');
        $pending = null;
        check($engine === 'turso' ? storageBytes($file) === $bytes : hash_file('sha256', $file) === $bytes[basename($file)], 'unused writer and TEMP changed persistent bytes');
        $reader->exec('BEGIN');
        check((int)$reader->query('SELECT count(*) FROM sample')->fetchColumn() === 1, 'initial snapshot');
        $writer = openDb($file);
        if ($mode === 'legacy') {
            try {
                $writer->exec('INSERT INTO sample VALUES(2)');
                throw new RuntimeException('legacy write ignored active reader');
            } catch (PDOException $e) {
                check((int)$e->errorInfo[1] === 5, 'legacy write must return BUSY');
            }
            $reader->exec('COMMIT');
        }
        $writer->exec('INSERT INTO sample VALUES(2)');
        if ($mode !== 'legacy') {
            check((int)$reader->query('SELECT count(*) FROM sample')->fetchColumn() === 1, 'old snapshot changed');
            $reader->exec('COMMIT');
        }
        check((int)$reader->query('SELECT count(*) FROM sample')->fetchColumn() === 2, 'fresh reader snapshot');
        try {
            $reader->exec('INSERT INTO sample VALUES(99)');
            throw new RuntimeException('promotion made readonly connection writable');
        } catch (PDOException $e) {
            check((int)$e->errorInfo[1] === 8, 'readonly policy after promotion');
        }
        if ($closeOrder === 'reader-first') { $reader = null; $writer = null; }
        else { $writer = null; $reader = null; }
        $reopened = openDb($file);
        check((int)$reopened->query('SELECT count(*) FROM sample')->fetchColumn() === 2, 'reopened committed rows');
        $reopened = null;
        echo $engine.' '.$mode.' '.$closeOrder." passed\n";
    }
}
echo $engine." PHP storage/callback probes passed\n";
