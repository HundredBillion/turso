<?php
declare(strict_types=1);

function reply(array $value): void {
    fwrite(STDOUT, json_encode($value, JSON_THROW_ON_ERROR | JSON_INVALID_UTF8_SUBSTITUTE)."\n");
    fflush(STDOUT);
}

final class WorkerDatabase {
    private PDO|SQLite3 $db;
    private string $api;
    public function __construct(string $uri, string $api, bool $readonly, int $extraFlags) {
        $this->api = $api;
        $flags = ($readonly ? 1 : 6) | 0x40 | $extraFlags;
        if ($api === 'pdo') {
            $options = [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_TIMEOUT => 0];
            if (defined('PDO::SQLITE_ATTR_OPEN_FLAGS')) { $options[PDO::SQLITE_ATTR_OPEN_FLAGS] = $flags; }
            $this->db = new PDO('sqlite:'.$uri, null, null, $options);
        } elseif ($api === 'sqlite3') {
            $this->db = new SQLite3($uri, $flags);
            $this->db->enableExceptions(true);
            $this->db->busyTimeout(0);
        } else { throw new RuntimeException('unknown PHP API'); }
    }
    public function query(string $sql, array $parameters = []): array {
        $statement = $this->db->prepare($sql);
        if ($this->api === 'pdo') {
            foreach ($parameters as $i => $value) {
                $type = $value === null ? PDO::PARAM_NULL : (is_int($value) ? PDO::PARAM_INT : PDO::PARAM_STR);
                $statement->bindValue($i + 1, $value, $type);
            }
            $statement->execute();
            $rows = $statement->fetchAll(PDO::FETCH_NUM);
            $statement->closeCursor();
        } else {
            foreach ($parameters as $i => $value) {
                $type = $value === null ? SQLITE3_NULL : (is_int($value) ? SQLITE3_INTEGER : SQLITE3_TEXT);
                $statement->bindValue($i + 1, $value, $type);
            }
            $result = $statement->execute();
            $rows = [];
            if ($result->numColumns() > 0) {
                while (($row = $result->fetchArray(SQLITE3_NUM)) !== false) { $rows[] = $row; }
            }
            $result->finalize();
            $statement->close();
        }
        return $rows;
    }
    public function errorCode(Throwable $error): int {
        return $this->api === 'pdo' && $error instanceof PDOException
            ? (int)($error->errorInfo[1] ?? 1) : ($this->api === 'sqlite3' ? $this->db->lastExtendedErrorCode() : 1);
    }
}

try {
    $config = json_decode($argv[1] ?? '', true, 512, JSON_THROW_ON_ERROR);
    $db = new WorkerDatabase($config['uri'], $config['api'], $config['readonly'] ?? false, $config['open_flag'] ?? 0);
    $maps = file_get_contents('/proc/self/maps');
    if ($maps === false) { throw new RuntimeException('Linux library mapping evidence unavailable'); }
    $libraries = array_values(array_unique(array_filter(array_map(function(string $line): string {
        $parts = preg_split('/\s+/', trim($line), 6);
        return count($parts) === 6 && str_contains($parts[5], 'sqlite') ? $parts[5] : '';
    }, explode("\n", $maps)))));
    $version = $db->query('SELECT sqlite_version()')[0][0];
    $turso = null;
    try { $turso = $db->query('SELECT turso_version()')[0][0]; } catch (Throwable $ignored) {}
    $identity = ['sqlite_version' => $version, 'turso_version' => $turso, 'libraries' => $libraries];
    if ($config['engine'] === 'turso') {
        $expected = realpath($config['library'] ?? '');
        if (!is_string($turso) || $expected === false || !in_array($expected, $libraries, true)) {
            throw new RuntimeException('expected Turso library is not mapped');
        }
        foreach ($libraries as $library) {
            if (str_contains(basename($library), 'libsqlite3.so') && realpath($library) !== $expected) {
                throw new RuntimeException('unexpected stock SQLite library mapped');
            }
        }
        $identity['sha256'] = hash_file('sha256', $expected);
    } elseif ($turso !== null) { throw new RuntimeException('SQLite control unexpectedly loaded Turso'); }
    if ($config['mode'] === 'shared') {
        $mode = $db->query('PRAGMA turso_mvcc_mode');
        if ($mode !== [['shared']]) {
            reply(['event' => 'unsupported', 'pid' => getmypid(), 'identity' => $identity,
                'reason' => 'explicit shared MVCC capability is absent; ordinary WAL is not acceptance']);
            exit(78);
        }
    }
    $db->query('PRAGMA busy_timeout='.(int)$config['busy_ms']);
    $db->query('PRAGMA synchronous=FULL');
    reply(['event' => 'ready', 'pid' => getmypid(), 'identity' => $identity,
        'journal_mode' => $db->query('PRAGMA journal_mode'), 'synchronous' => $db->query('PRAGMA synchronous'),
        'busy_timeout' => $db->query('PRAGMA busy_timeout')]);
    while (($line = fgets(STDIN)) !== false) {
        $request = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
        $start = hrtime(true);
        try {
            if ($request['op'] === 'ping') { $rows = [[getmypid(), $request['token']]]; }
            elseif ($request['op'] === 'close') { reply(['id' => $request['id'], 'ok' => true, 'rows' => []]); exit(0); }
            elseif (in_array($request['op'], ['sql', 'sql_then_exit', 'sql_with_start'], true)) {
                if ($request['op'] === 'sql_with_start') { reply(['event' => 'executing', 'id' => $request['id']]); }
                $rows = $db->query($request['sql'], $request['parameters'] ?? []);
                if ($request['op'] === 'sql_then_exit') {
                    if (!function_exists('posix_kill')) { throw new RuntimeException('POSIX signal support required'); }
                    posix_kill(getmypid(), 9);
                    throw new RuntimeException('SIGKILL unexpectedly returned');
                }
            }
            else { throw new RuntimeException('unknown worker operation'); }
            reply(['id' => $request['id'], 'ok' => true, 'rows' => $rows,
                'duration_ns' => hrtime(true) - $start, 'attempts' => 1]);
        } catch (Throwable $error) {
            reply(['id' => $request['id'], 'ok' => false, 'code' => $db->errorCode($error),
                'message' => $error->getMessage(), 'duration_ns' => hrtime(true) - $start, 'attempts' => 1]);
        }
    }
} catch (Throwable $error) {
    reply(['event' => 'fatal', 'pid' => getmypid(), 'message' => $error->getMessage()]);
    exit(1);
}
