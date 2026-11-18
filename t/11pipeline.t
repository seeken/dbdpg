#!perl

## Test pipeline mode functionality (PostgreSQL 14+)

use 5.008001;
use strict;
use warnings;
use lib 'blib/lib', 'blib/arch', 't';
use DBD::Pg qw(:async :pg_pipeline);
use Test::More;
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! $dbh) {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}

my $pgversion = $dbh->{pg_lib_version};

if ($pgversion < 140000) {
    plan skip_all => 'Pipeline mode requires PostgreSQL 14 or later';
}

# New headers may be paired with an older runtime library.
eval { $dbh->pg_enter_pipeline_mode(); };
if ($@) {
    die $@ if $@ !~ /requires runtime libpq 14/;
    $dbh->disconnect();
    plan skip_all => 'Pipeline mode unavailable in runtime libpq';
}
$dbh->pg_exit_pipeline_mode();

plan 'no_plan';

my ($result, $expected, $t);

$t='pg_pipeline_status returns 0 (off) by default';
my $status = $dbh->pg_pipeline_status();
is ($status, 0, $t);

# Enter pipeline mode

$t='pg_enter_pipeline_mode succeeds';
$result = $dbh->pg_enter_pipeline_mode();
is ($result, 1, $t);

$t='pg_pipeline_status returns 1 (on) after entering pipeline mode';
$status = $dbh->pg_pipeline_status();
is ($status, 1, $t);

$t='pg_enter_pipeline_mode is idempotent';
$result = $dbh->pg_enter_pipeline_mode();
is ($result, 1, $t);

# Exit pipeline mode

$t='pg_exit_pipeline_mode succeeds';
$result = $dbh->pg_exit_pipeline_mode();
is ($result, 1, $t);

$t='pg_pipeline_status returns 0 (off) after exiting pipeline mode';
$status = $dbh->pg_pipeline_status();
is ($status, 0, $t);

$t='pg_exit_pipeline_mode is idempotent when not in pipeline mode';
$result = $dbh->pg_exit_pipeline_mode();
is ($result, 1, $t);

$t='Normal queries work after entering and exiting pipeline mode';
eval {
    $dbh->do('SELECT 1');
};
is ($@, q{}, $t);

# pg_pipeline_sync

$t='pg_pipeline_sync fails when not in pipeline mode';
eval {
    $dbh->pg_pipeline_sync();
};
ok ($@, $t);

# pg_pipeline_sync + pg_getresult: basic sync cycle

$dbh->pg_enter_pipeline_mode();

$t='pg_pipeline_sync succeeds in pipeline mode';
$result = $dbh->pg_pipeline_sync();
is ($result, 1, $t);

$t='pg_getresult returns pipeline sync result';
$result = $dbh->pg_getresult();
is (ref $result, 'HASH', $t);

$t='Pipeline sync result has status PGRES_PIPELINE_SYNC';
is ($result->{status}, PGRES_PIPELINE_SYNC, $t);

$t='pg_getresult returns undef after sync result';
my $null_result = $dbh->pg_getresult();
ok (!defined $null_result, $t);

$dbh->pg_exit_pipeline_mode();

# pg_getresult when not in pipeline mode

$t='pg_getresult returns undef when no results pending';
$null_result = $dbh->pg_getresult();
ok (!defined $null_result, $t);

# Verify connection is still good

$t='Connection works after sync cycle';
eval {
    $dbh->do('SELECT 1');
};
is ($@, q{}, $t);

$t='Enter/exit pipeline still works after sync cycle';
$dbh->pg_enter_pipeline_mode();
$dbh->pg_pipeline_sync();
$dbh->pg_getresult();  # PIPELINE_SYNC
$dbh->pg_getresult();  # NULL
$dbh->pg_exit_pipeline_mode();
eval { $dbh->do('SELECT 2'); };
is ($@, q{}, $t);

# pg_send_query_params: INSERT pipeline

$dbh->do('CREATE TEMP TABLE dbd_pg_test_pipeline(id integer, name text)');

$t='pg_send_query_params queues an INSERT';
$dbh->pg_enter_pipeline_mode();
$result = $dbh->pg_send_query_params(
    'INSERT INTO dbd_pg_test_pipeline(id, name) VALUES ($1, $2)',
    [1, 'Alice']
);
is ($result, 1, $t);

$t='pg_send_query_params queues a second INSERT';
$result = $dbh->pg_send_query_params(
    'INSERT INTO dbd_pg_test_pipeline(id, name) VALUES ($1, $2)',
    [2, 'Bob']
);
is ($result, 1, $t);

$t='pg_pipeline_sync after INSERTs succeeds'; # nospellcheck
$result = $dbh->pg_pipeline_sync();
is ($result, 1, $t);

# Collect results: INSERT1 -> NULL -> INSERT2 -> NULL -> SYNC

$t='First INSERT result is COMMAND_OK';
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_COMMAND_OK, $t);  # PGRES_COMMAND_OK

$t='NULL separator after first INSERT';
my $null_result2 = $dbh->pg_getresult();
ok (!defined $null_result2, $t);

$t='Second INSERT result is COMMAND_OK';
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_COMMAND_OK, $t);

$t='NULL separator after second INSERT';
$null_result2 = $dbh->pg_getresult();
ok (!defined $null_result2, $t);

$t='PIPELINE_SYNC result';
$result = $dbh->pg_getresult();
ok (defined $result && $result->{status} == PGRES_PIPELINE_SYNC, $t);

$dbh->pg_exit_pipeline_mode();

$t='Pipeline INSERTs committed the data'; # nospellcheck
my $rows = $dbh->selectall_arrayref(
    'SELECT id, name FROM dbd_pg_test_pipeline ORDER BY id'
);
$expected = [[1, 'Alice'], [2, 'Bob']];
is_deeply ($rows, $expected, $t);

# pg_send_query_params: SELECT in pipeline

$dbh->pg_enter_pipeline_mode();

$t='pg_send_query_params queues a SELECT';
$result = $dbh->pg_send_query_params(
    'SELECT id, name FROM dbd_pg_test_pipeline WHERE id = $1',
    [1]
);
is ($result, 1, $t);

$dbh->pg_pipeline_sync();

$t='SELECT result is TUPLES_OK';
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_TUPLES_OK, $t);

$t='SELECT result has correct ntuples';
is ($result->{ntuples}, 1, $t);

$t='SELECT result has correct nfields';
is ($result->{nfields}, 2, $t);

$t='SELECT result rows contain correct data';
is_deeply ($result->{rows}, [['1', 'Alice']], $t);

$null_result = $dbh->pg_getresult();  # NULL separator
$result = $dbh->pg_getresult();       # PIPELINE_SYNC
$dbh->pg_exit_pipeline_mode();

# Pipeline error handling

$dbh->pg_enter_pipeline_mode();

$t='Send a query that will fail (syntax error)';
$result = $dbh->pg_send_query_params('GARBAGE SQL', []);
is ($result, 1, $t);

$t='Send a valid query after the bad one';
$result = $dbh->pg_send_query_params('SELECT 1 AS x', []);
is ($result, 1, $t);

$dbh->pg_pipeline_sync();

$t='First result is FATAL_ERROR';
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_FATAL_ERROR, $t);

$t='Error result has error message';
ok (defined $result->{error}, $t);

$null_result = $dbh->pg_getresult();  # NULL separator

$t='Second result is PIPELINE_ABORTED';
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_PIPELINE_ABORTED, $t);

$null_result = $dbh->pg_getresult();  # NULL separator

$t='PIPELINE_SYNC after error recovery';
$result = $dbh->pg_getresult();
ok (defined $result && $result->{status} == PGRES_PIPELINE_SYNC, $t);

$dbh->pg_exit_pipeline_mode();

# Pipeline errors abort the implicit transaction, so we must rollback
$dbh->do('ROLLBACK');

$t='Connection works after pipeline error';
eval {
    $dbh->do('SELECT 1');
};
is ($@, q{}, $t);

# pg_send_prepare + pg_send_query_prepared
# The ROLLBACK above cleared the aborted transaction, which also rolled back
# the temp table and its data. Recreate and repopulate for the next tests.
$dbh->do('CREATE TEMP TABLE dbd_pg_test_pipeline(id integer, name text)');
$dbh->do(q{INSERT INTO dbd_pg_test_pipeline(id, name) VALUES (1, 'Alice'), (2, 'Bob')});

$dbh->pg_enter_pipeline_mode();

$t='pg_send_prepare queues a PREPARE';
$result = $dbh->pg_send_prepare(
    'pipeline_insert',
    'INSERT INTO dbd_pg_test_pipeline(id, name) VALUES ($1, $2)'
);
is ($result, 1, $t);

$t='pg_send_query_prepared queues execution';
$result = $dbh->pg_send_query_prepared('pipeline_insert', [3, 'Charlie']);
is ($result, 1, $t);

$t='pg_send_query_prepared queues second execution';
$result = $dbh->pg_send_query_prepared('pipeline_insert', [4, 'Diana']);
is ($result, 1, $t);

$dbh->pg_pipeline_sync();

# Collect: PREPARE -> NULL -> INSERT1 -> NULL -> INSERT2 -> NULL -> SYNC
$t='PREPARE result is COMMAND_OK';
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_COMMAND_OK, $t);
$dbh->pg_getresult();  # NULL

$t='First prepared INSERT is COMMAND_OK';
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_COMMAND_OK, $t);
$dbh->pg_getresult();  # NULL

$t='Second prepared INSERT is COMMAND_OK';
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_COMMAND_OK, $t);
$dbh->pg_getresult();  # NULL

$t='PIPELINE_SYNC after prepared statements';
$result = $dbh->pg_getresult();
ok (defined $result && $result->{status} == PGRES_PIPELINE_SYNC, $t);

$dbh->pg_exit_pipeline_mode();

$t='Prepared statement data committed';
$rows = $dbh->selectall_arrayref(
    'SELECT id, name FROM dbd_pg_test_pipeline ORDER BY id'
);
$expected = [[1, 'Alice'], [2, 'Bob'], [3, 'Charlie'], [4, 'Diana']];
is_deeply ($rows, $expected, $t);

# Deallocate the prepared statement
$dbh->do('DEALLOCATE pipeline_insert');

# pg_send_flush_request

$t='pg_send_flush_request fails outside pipeline mode';
eval {
    $dbh->pg_send_flush_request();
};
ok ($@, $t);

$dbh->pg_enter_pipeline_mode();

$t='pg_send_flush_request succeeds in pipeline mode';
$dbh->pg_send_query_params('SELECT 42 AS answer', []);
$result = $dbh->pg_send_flush_request();
is ($result, 1, $t);

# Flush the output buffer and collect result
$dbh->pg_flush();

$t='Flush request lets us retrieve result without sync';
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_TUPLES_OK, $t);
is_deeply ($result->{rows}, [['42']], "$t data correct");

$dbh->pg_getresult();  # NULL separator

# Need a sync to cleanly exit
$dbh->pg_pipeline_sync();
$dbh->pg_getresult();  # PIPELINE_SYNC
$dbh->pg_exit_pipeline_mode();

# Guards: COPY rejected in pipeline mode

$dbh->pg_enter_pipeline_mode();

$t='pg_putcopydata croaks in pipeline mode';
eval {
    $dbh->pg_putcopydata("data\n");
};
like ($@, qr{pipeline}, $t);

$t='pg_putcopyend croaks in pipeline mode';
eval {
    $dbh->pg_putcopyend();
};
like ($@, qr{pipeline}, $t);

$t='pg_getcopydata croaks in pipeline mode';
my $copy_buf;
eval {
    $dbh->pg_getcopydata($copy_buf);
};
like ($@, qr{pipeline}, $t);

$t='pg_putcopydata_async croaks in pipeline mode';
eval {
    $dbh->pg_putcopydata_async("data\n");
};
like ($@, qr{pipeline}, $t);

$t='pg_putcopyend_async croaks in pipeline mode';
eval {
    $dbh->pg_putcopyend_async();
};
like ($@, qr{pipeline}, $t);

$t='pg_getcopydata_async croaks in pipeline mode';
eval {
    $dbh->pg_getcopydata_async($copy_buf);
};
like ($@, qr{pipeline}, $t);

$dbh->pg_exit_pipeline_mode();

# Exit with pending results

$dbh->pg_enter_pipeline_mode();
$dbh->pg_send_query_params('SELECT 1', []);
$dbh->pg_pipeline_sync();

$t='pg_exit_pipeline_mode fails with unconsumed results';
eval {
    $result = $dbh->pg_exit_pipeline_mode();
};
ok ($@, $t);

# Drain results before exiting
$dbh->pg_getresult();  # SELECT result
$dbh->pg_getresult();  # NULL
$dbh->pg_getresult();  # PIPELINE_SYNC

$t='pg_exit_pipeline_mode succeeds after draining results';
$result = $dbh->pg_exit_pipeline_mode();
is ($result, 1, $t);

# Multiple pipeline cycles without exiting

$dbh->pg_exit_pipeline_mode();
eval { $dbh->do('DROP TABLE IF EXISTS dbd_pg_test_pipeline_multi'); };
$dbh->do('CREATE TEMP TABLE dbd_pg_test_pipeline_multi(id integer, name text)');

$dbh->pg_enter_pipeline_mode();

$t='First pipeline cycle: INSERT';
$dbh->pg_send_query_params(
    'INSERT INTO dbd_pg_test_pipeline_multi(id, name) VALUES ($1, $2)',
    [10, 'Cycle1']
);
$dbh->pg_pipeline_sync();

$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_COMMAND_OK, $t);
$dbh->pg_getresult();  # NULL
$dbh->pg_getresult();  # PIPELINE_SYNC

$t='Second pipeline cycle: SELECT';
$dbh->pg_send_query_params(
    'SELECT name FROM dbd_pg_test_pipeline_multi WHERE id = $1',
    [10]
);
$dbh->pg_pipeline_sync();

$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_TUPLES_OK, $t);
is_deeply ($result->{rows}, [['Cycle1']], "$t correct data");
$dbh->pg_getresult();  # NULL
$dbh->pg_getresult();  # PIPELINE_SYNC

$dbh->pg_exit_pipeline_mode();

# Large pipeline: 100 INSERTs

$dbh->pg_exit_pipeline_mode() if $dbh->pg_pipeline_status();
eval { $dbh->do('DROP TABLE IF EXISTS dbd_pg_test_pipeline_large'); };
$dbh->do('CREATE TEMP TABLE dbd_pg_test_pipeline_large(id integer, name text)');

$dbh->pg_enter_pipeline_mode();

$t='Large pipeline: 100 INSERTs queued'; # nospellcheck
my $large_ok = 1;
for my $i (1..100) {
    my $r = $dbh->pg_send_query_params(
        'INSERT INTO dbd_pg_test_pipeline_large(id, name) VALUES ($1, $2)',
        [$i, "Row$i"]
    );
    if (!$r) { $large_ok = 0; last; }
}
ok ($large_ok, $t);

$dbh->pg_pipeline_sync();

$t='Large pipeline: drain all 100 results';
my $drain_ok = 1;
for my $i (1..100) {
    my $r = $dbh->pg_getresult();
    if (!$r || $r->{status} != PGRES_COMMAND_OK) { $drain_ok = 0; last; }
    $dbh->pg_getresult();  # NULL separator
}
ok ($drain_ok, $t);

$result = $dbh->pg_getresult();  # PIPELINE_SYNC
ok (defined $result && $result->{status} == PGRES_PIPELINE_SYNC, "$t sync");

$dbh->pg_exit_pipeline_mode();

$t='All 100 rows inserted';
$rows = $dbh->selectall_arrayref('SELECT count(*) FROM dbd_pg_test_pipeline_large');
is ($rows->[0][0], 100, $t);

# SELECT returning zero rows

$dbh->pg_enter_pipeline_mode();

$t='SELECT zero rows returns TUPLES_OK';
$dbh->pg_send_query_params(
    'SELECT id, name FROM dbd_pg_test_pipeline WHERE id = $1',
    [99999]
);
$dbh->pg_pipeline_sync();

$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_TUPLES_OK, $t);
is ($result->{ntuples}, 0, "$t with ntuples 0");
is ($result->{nfields}, 2, "$t with correct nfields");
ok (!exists $result->{rows}, "$t has no rows key");

$dbh->pg_getresult();  # NULL
$dbh->pg_getresult();  # PIPELINE_SYNC
$dbh->pg_exit_pipeline_mode();

# NULL parameter values

$dbh->pg_enter_pipeline_mode();

$t='pg_send_query_params handles undef (NULL) params';
$result = $dbh->pg_send_query_params(
    'SELECT $1::text AS val',
    [undef]
);
is ($result, 1, $t);

$dbh->pg_pipeline_sync();

$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_TUPLES_OK, "$t returns TUPLES_OK");
is ($result->{rows}[0][0], undef, "$t NULL value returned as undef");

$dbh->pg_getresult();  # NULL
$dbh->pg_getresult();  # PIPELINE_SYNC
$dbh->pg_exit_pipeline_mode();

# pg_pipeline_status returns 2 (aborted) during error

$dbh->pg_enter_pipeline_mode();

$dbh->pg_send_query_params('GARBAGE', []);
$dbh->pg_pipeline_sync();

$dbh->pg_getresult();  # FATAL_ERROR
$dbh->pg_getresult();  # NULL

$t='pg_pipeline_status returns 2 (aborted) after error';
$status = $dbh->pg_pipeline_status();
is ($status, 2, $t);

$dbh->pg_getresult();  # PIPELINE_SYNC

$t='pg_pipeline_status returns 1 (on) after sync clears abort';
$status = $dbh->pg_pipeline_status();
is ($status, 1, $t);

$dbh->pg_exit_pipeline_mode();
$dbh->do('ROLLBACK');

# pg_flush return value

$dbh->pg_enter_pipeline_mode();

$dbh->pg_send_query_params('SELECT 1', []);
$dbh->pg_send_flush_request();

$t='pg_flush returns 0 (flushed) or 1 (pending)';
my $flush_result = $dbh->pg_flush();
ok ($flush_result == 0 || $flush_result == 1, $t);

# Drain
$dbh->pg_getresult();  # SELECT result
$dbh->pg_getresult();  # NULL
$dbh->pg_pipeline_sync();
$dbh->pg_getresult();  # PIPELINE_SYNC
$dbh->pg_exit_pipeline_mode();

# pg_send_query_params with no params

$dbh->pg_enter_pipeline_mode();

$t='pg_send_query_params works without params argument';
$result = $dbh->pg_send_query_params('SELECT 42 AS answer');
is ($result, 1, $t);

$dbh->pg_pipeline_sync();

$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_TUPLES_OK, "$t returns TUPLES_OK");
is_deeply ($result->{rows}, [['42']], "$t correct data");

$dbh->pg_getresult();  # NULL
$dbh->pg_getresult();  # PIPELINE_SYNC
$dbh->pg_exit_pipeline_mode();

# Two consecutive syncs

$dbh->pg_enter_pipeline_mode();

$dbh->pg_send_query_params('SELECT 1 AS a', []);
$dbh->pg_pipeline_sync();
$dbh->pg_send_query_params('SELECT 2 AS b', []);
$dbh->pg_pipeline_sync();

# Drain first pipeline: result -> NULL -> SYNC
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_TUPLES_OK, 'First sync group: TUPLES_OK');
$dbh->pg_getresult();  # NULL
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_PIPELINE_SYNC, 'First PIPELINE_SYNC');

# Drain second pipeline: result -> NULL -> SYNC
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_TUPLES_OK, 'Second sync group: TUPLES_OK');
$dbh->pg_getresult();  # NULL
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_PIPELINE_SYNC, 'Second PIPELINE_SYNC');

$dbh->pg_exit_pipeline_mode();

# ? placeholder conversion

eval { $dbh->do('DROP TABLE IF EXISTS dbd_pg_test_pipeline_qmark'); };
$dbh->do('CREATE TEMP TABLE dbd_pg_test_pipeline_qmark(id integer, name text)');

$dbh->pg_enter_pipeline_mode();

$t='pg_send_query_params accepts ? placeholders';
$result = $dbh->pg_send_query_params(
    'INSERT INTO dbd_pg_test_pipeline_qmark(id, name) VALUES (?, ?)',
    [1, 'Alice']
);
is ($result, 1, $t);

$t='pg_send_query_params accepts ? for SELECT';
$result = $dbh->pg_send_query_params(
    'SELECT id, name FROM dbd_pg_test_pipeline_qmark WHERE id = ?',
    [1]
);
is ($result, 1, $t);

$dbh->pg_pipeline_sync();

$t='? INSERT result is COMMAND_OK';
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_COMMAND_OK, $t);
$dbh->pg_getresult();  # NULL

$t='? SELECT result has correct data';
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_TUPLES_OK, $t);
is_deeply ($result->{rows}, [['1', 'Alice']], "$t rows match");
$dbh->pg_getresult();  # NULL
$dbh->pg_getresult();  # PIPELINE_SYNC

$dbh->pg_exit_pipeline_mode();

# $1 placeholders still work (not converted)

$dbh->pg_enter_pipeline_mode();

$t='$1 placeholders still work alongside ? support';
$result = $dbh->pg_send_query_params(
    'INSERT INTO dbd_pg_test_pipeline_qmark(id, name) VALUES ($1, $2)',
    [2, 'Bob']
);
is ($result, 1, $t);

$dbh->pg_pipeline_sync();
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_COMMAND_OK, $t);
$dbh->pg_getresult();  # NULL
$dbh->pg_getresult();  # PIPELINE_SYNC

$dbh->pg_exit_pipeline_mode();

$t='Both ? and $1 data committed correctly';
$rows = $dbh->selectall_arrayref(
    'SELECT id, name FROM dbd_pg_test_pipeline_qmark ORDER BY id'
);
$expected = [[1, 'Alice'], [2, 'Bob']];
is_deeply ($rows, $expected, $t);

# ? inside single-quoted string is preserved

$dbh->pg_enter_pipeline_mode();

$t='? inside single-quoted string is not converted';
$result = $dbh->pg_send_query_params(
    q{SELECT 'what?' AS question, ? AS param},
    ['hello']
);
is ($result, 1, $t);
$dbh->pg_pipeline_sync();

$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_TUPLES_OK, "$t TUPLES_OK");
is ($result->{rows}[0][0], 'what?', "$t literal preserved");
is ($result->{rows}[0][1], 'hello', "$t param bound correctly");

$dbh->pg_getresult();  # NULL
$dbh->pg_getresult();  # SYNC
$dbh->pg_exit_pipeline_mode();

# ? inside double-quoted identifier - use a column alias

$dbh->pg_enter_pipeline_mode();

$t='? inside double-quoted identifier is not converted';
$result = $dbh->pg_send_query_params(
    q{SELECT ? AS "what?"},
    ['test']
);
is ($result, 1, $t);
$dbh->pg_pipeline_sync();

$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_TUPLES_OK, "$t TUPLES_OK");
is ($result->{rows}[0][0], 'test', "$t param bound correctly");

$dbh->pg_getresult();  # NULL
$dbh->pg_getresult();  # SYNC
$dbh->pg_exit_pipeline_mode();

# ? inside dollar-quoted string

$dbh->pg_enter_pipeline_mode();

$t='? inside dollar-quoted string is not converted';
$result = $dbh->pg_send_query_params(
    q{SELECT $$what?$$ AS question, ? AS param},
    ['hello']
);
is ($result, 1, $t);
$dbh->pg_pipeline_sync();

$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_TUPLES_OK, "$t TUPLES_OK");
is ($result->{rows}[0][0], 'what?', "$t dollar-quoted preserved");
is ($result->{rows}[0][1], 'hello', "$t param bound correctly");

$dbh->pg_getresult();  # NULL
$dbh->pg_getresult();  # SYNC
$dbh->pg_exit_pipeline_mode();

# ? inside line comment

$dbh->pg_enter_pipeline_mode();

$t='? inside line comment is not converted';
$result = $dbh->pg_send_query_params(
    "SELECT ? AS val -- is this a question?\n",
    [42]
);
is ($result, 1, $t);
$dbh->pg_pipeline_sync();

$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_TUPLES_OK, "$t TUPLES_OK");
is ($result->{rows}[0][0], '42', "$t param bound correctly");

$dbh->pg_getresult();  # NULL
$dbh->pg_getresult();  # SYNC
$dbh->pg_exit_pipeline_mode();

# ? inside block comment

$dbh->pg_enter_pipeline_mode();

$t='? inside block comment is not converted';
$result = $dbh->pg_send_query_params(
    'SELECT ? AS val /* what? really? */',
    [99]
);
is ($result, 1, $t);
$dbh->pg_pipeline_sync();

$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_TUPLES_OK, "$t TUPLES_OK");
is ($result->{rows}[0][0], '99', "$t param bound correctly");

$dbh->pg_getresult();  # NULL
$dbh->pg_getresult();  # SYNC
$dbh->pg_exit_pipeline_mode();

# Multiple ? convert to correct $1, $2, $3 sequence

$dbh->pg_enter_pipeline_mode();

$t='Multiple ? placeholders convert to correct sequence';
$result = $dbh->pg_send_query_params(
    'SELECT ? AS a, ? AS b, ? AS c',
    ['x', 'y', 'z']
);
is ($result, 1, $t);
$dbh->pg_pipeline_sync();

$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_TUPLES_OK, "$t TUPLES_OK");
is_deeply ($result->{rows}, [['x', 'y', 'z']], "$t all params correct");

$dbh->pg_getresult();  # NULL
$dbh->pg_getresult();  # SYNC
$dbh->pg_exit_pipeline_mode();

# Dollar placeholders pass through unchanged

$dbh->pg_enter_pipeline_mode();

$t='Dollar placeholder passes through unchanged';
$result = $dbh->pg_send_query_params(
    'SELECT $1 AS val',
    ['works']
);
is ($result, 1, $t);
$dbh->pg_pipeline_sync();

$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_TUPLES_OK, "$t TUPLES_OK");
is ($result->{rows}[0][0], 'works', "$t \$1 param correct");

$dbh->pg_getresult();  # NULL
$dbh->pg_getresult();  # SYNC
$dbh->pg_exit_pipeline_mode();

# DBI transactions survive sync: only the caller's result is visible.
$dbh->commit();
$dbh->{AutoCommit} = 1;
$dbh->do('CREATE TABLE dbd_pg_test_pipeline_txn (id integer)');
for my $finish (qw(rollback commit)) {
    $dbh->{AutoCommit} = 0;
    $dbh->pg_enter_pipeline_mode();
    $dbh->pg_send_query_params('INSERT INTO dbd_pg_test_pipeline_txn VALUES (?)', [1]);
    $dbh->pg_pipeline_sync();
    $result = $dbh->pg_getresult();
    is ($result->{status}, PGRES_COMMAND_OK, "$finish sees INSERT result first");
    is ($result->{cmdtuples}, '1', "$finish sees INSERT row count");
    is ($dbh->pg_getresult(), undef, "$finish sees one separator");
    is ($dbh->pg_getresult()->{status}, PGRES_PIPELINE_SYNC, "$finish sees sync next");
    $dbh->pg_exit_pipeline_mode();
    if ($finish eq 'rollback') { $dbh->rollback(); }
    else { $dbh->commit(); }
    $dbh->{AutoCommit} = 1;
    is ($dbh->selectrow_array('SELECT count(*) FROM dbd_pg_test_pipeline_txn'),
        $finish eq 'rollback' ? 0 : 1, "$finish has the expected effect");
}

$dbh->begin_work();
$dbh->pg_enter_pipeline_mode();
$dbh->pg_send_prepare('pipeline_txn', 'INSERT INTO dbd_pg_test_pipeline_txn VALUES ($1)');
$dbh->pg_send_query_prepared('pipeline_txn', [2]);
$dbh->pg_pipeline_sync();
for my $command (qw(prepare insert)) {
    is ($dbh->pg_getresult()->{status}, PGRES_COMMAND_OK, "begin_work $command result");
    $dbh->pg_getresult();
}
$dbh->pg_getresult();
$dbh->pg_exit_pipeline_mode();
$dbh->rollback();
ok ($dbh->{AutoCommit}, 'begin_work rollback restores AutoCommit');
is ($dbh->selectrow_array('SELECT count(*) FROM dbd_pg_test_pipeline_txn'), 1,
    'begin_work pipeline insert was rolled back');

# Prepared execution can also be the first command in a fresh transaction.
$dbh->{AutoCommit} = 0;
$dbh->pg_enter_pipeline_mode();
$dbh->pg_send_query_prepared('pipeline_txn', [3]);
$dbh->pg_pipeline_sync();
is ($dbh->pg_getresult()->{status}, PGRES_COMMAND_OK, 'Prepared execution starts deferred transaction');
$dbh->pg_getresult();
$dbh->pg_getresult();
$dbh->pg_exit_pipeline_mode();
$dbh->rollback();
$dbh->{AutoCommit} = 1;
is ($dbh->selectrow_array('SELECT count(*) FROM dbd_pg_test_pipeline_txn'), 1,
    'Prepared execution can be rolled back');
$dbh->do('DEALLOCATE pipeline_txn');

$dbh->begin_work();
$dbh->{ReadOnly} = 1;
$dbh->pg_enter_pipeline_mode();
$dbh->pg_send_query_params('SHOW transaction_read_only');
$dbh->pg_send_query_params('INSERT INTO dbd_pg_test_pipeline_txn VALUES (4)');
$dbh->pg_pipeline_sync();
is_deeply ($dbh->pg_getresult()->{rows}, [['on']], 'Pipeline honors ReadOnly');
$dbh->pg_getresult();
is ($dbh->pg_getresult()->{status}, PGRES_FATAL_ERROR, 'ReadOnly rejects pipeline writes');
$dbh->pg_getresult();
$dbh->pg_getresult();
$dbh->pg_exit_pipeline_mode();
$dbh->rollback();
$dbh->{ReadOnly} = 0;
$dbh->do('DROP TABLE dbd_pg_test_pipeline_txn');

# A late AutoCommit change cannot make the hidden BEGIN consume prior results.
$dbh->pg_enter_pipeline_mode();
$dbh->pg_send_query_params('SELECT 123');
$dbh->{AutoCommit} = 0;
eval { $dbh->pg_send_query_params('SELECT 456'); };
like ($@, qr{Cannot start a deferred DBI transaction in a busy pipeline},
    'Late transaction start is rejected before adding a hidden command');
$dbh->pg_pipeline_sync();
is_deeply ($dbh->pg_getresult()->{rows}, [['123']], 'Earlier caller result is preserved');
$dbh->pg_getresult();
$dbh->pg_getresult();
$dbh->pg_exit_pipeline_mode();
$dbh->rollback();
$dbh->{AutoCommit} = 1;
# Placeholder scanning follows the normal driver's escape/settings rules.
for my $case (
    [q{SELECT '$1.00'::text, ?::text}, [['$1.00', 'bound']]],
    [q{SELECT $body$BEGIN RETURN $1; END$body$::text, ?::text},
        [['BEGIN RETURN $1; END', 'bound']]],
    [qq{SELECT ?::text -- \$1 ?\n}, [['bound']]],
    [q{SELECT ?::text /* $1 /* nested ? */ $2 */}, [['bound']]],
    [q{SELECT ?::text AS "$1"}, [['bound']]],
    [q{SELECT '{"k":1}'::jsonb \? ?}, [['t']]],
    [q{SELECT '{"k":1}'::jsonb \?| ARRAY[?]}, [['t']]],
    [q{SELECT '{"k":1}'::jsonb \?& ARRAY[?]}, [['t']]],
) {
    my ($sql, $want) = @{$case};
    $dbh->pg_enter_pipeline_mode();
    $dbh->pg_send_query_params($sql, [$sql =~ /jsonb/ ? 'k' : 'bound']);
    $dbh->pg_pipeline_sync();
    is_deeply ($dbh->pg_getresult()->{rows}, $want, "Placeholder scan: $sql");
    $dbh->pg_getresult();
    $dbh->pg_getresult();
    $dbh->pg_exit_pipeline_mode();
}
for my $sql ('SELECT ?, $1', 'SELECT $1, ?') {
    $dbh->pg_enter_pipeline_mode();
    eval { $dbh->pg_send_query_params($sql, [1, 2]); };
    like ($@, qr{Cannot mix placeholder styles}, 'Mixed placeholder styles rejected');
    eval { $dbh->pg_send_prepare('pipeline_mixed', $sql); };
    like ($@, qr{Cannot mix placeholder styles}, 'Mixed prepare placeholders rejected');
    $dbh->pg_exit_pipeline_mode();
}
$dbh->{pg_placeholder_dollaronly} = 1;
$dbh->pg_enter_pipeline_mode();
$dbh->pg_send_query_params(q{SELECT '{"k":1}'::jsonb ? $1}, ['k']);
$dbh->pg_send_prepare('pipeline_json', q{SELECT '{"k":1}'::jsonb ? $1});
$dbh->pg_send_query_prepared('pipeline_json', ['k']);
$dbh->pg_pipeline_sync();
is_deeply ($dbh->pg_getresult()->{rows}, [['t']], 'Dollar only preserves JSON operator');
$dbh->pg_getresult();
is ($dbh->pg_getresult()->{status}, PGRES_COMMAND_OK, 'Dollar only prepare succeeds');
$dbh->pg_getresult();
is_deeply ($dbh->pg_getresult()->{rows}, [['t']], 'Dollar only prepared query succeeds');
$dbh->pg_getresult();
$dbh->pg_getresult();
$dbh->pg_exit_pipeline_mode();
$dbh->{pg_placeholder_dollaronly} = 0;
$dbh->do('DEALLOCATE pipeline_json');

# Input conversion and result flags match normal binding and fetch.
for my $encoding (qw(UTF8 LATIN1)) {
    $dbh->do("SET client_encoding TO $encoding");
    $dbh->{pg_enable_utf8} = -1;
    for my $flag (0, 1) {
        my $value = "caf\x{e9}";
        utf8::upgrade($value) if $flag;
        my $normal = $dbh->selectrow_array('SELECT ?::text', undef, $value);
        $dbh->pg_enter_pipeline_mode();
        $dbh->pg_send_query_params('SELECT ?::text', [$value]);
        $dbh->pg_send_prepare('pipeline_encoding', 'SELECT ?::text');
        $dbh->pg_send_query_prepared('pipeline_encoding', [$value]);
        $dbh->pg_pipeline_sync();
        for my $command (qw(params prepare prepared)) {
            $result = $dbh->pg_getresult();
            if ($command ne 'prepare') {
                is ($result->{rows}[0][0], $normal, "$encoding $flag $command round trip");
                is (!!utf8::is_utf8($result->{rows}[0][0]), !!utf8::is_utf8($normal),
                    "$encoding $flag $command result flag");
            }
            $dbh->pg_getresult();
        }
        $dbh->pg_getresult();
        $dbh->pg_exit_pipeline_mode();
        is (!!utf8::is_utf8($value), !!$flag, 'Binding preserves input flag');
        $dbh->do('DEALLOCATE pipeline_encoding');
    }
}
$dbh->do('SET client_encoding TO UTF8');
$dbh->{pg_enable_utf8} = -1;
for my $utf8 (0, 1) {
    $dbh->{pg_enable_utf8} = $utf8;
    my $normal = $dbh->selectrow_array(q{SELECT 'plain'::text});
    $dbh->pg_enter_pipeline_mode();
    $dbh->pg_send_query_params(q{SELECT 'plain'::text});
    $dbh->pg_pipeline_sync();
    $result = $dbh->pg_getresult();
    is (!!utf8::is_utf8($result->{rows}[0][0]), !!utf8::is_utf8($normal),
        'Explicit encoding flag matches normal fetch');
    $dbh->pg_getresult();
    $dbh->pg_getresult();
    $dbh->pg_exit_pipeline_mode();
}
$dbh->{pg_enable_utf8} = -1;
my $sql_value = "caf\x{e9}";
utf8::upgrade($sql_value);
$dbh->pg_enter_pipeline_mode();
$dbh->pg_send_query_params("SELECT '$sql_value'::text");
$dbh->pg_pipeline_sync();
is ($dbh->pg_getresult()->{rows}[0][0], $sql_value, 'SQL text uses client encoding');
$dbh->pg_getresult();
$dbh->pg_getresult();
$dbh->pg_exit_pipeline_mode();

$dbh->pg_enter_pipeline_mode();
for my $send (qw(pg_send_query_params pg_send_query_prepared)) {
    eval { $dbh->$send('SELECT ?::text', ["a\0b"]); };
    like ($@, qr{cannot contain NUL}, "$send rejects embedded NUL before sending");
}
$dbh->pg_send_query_params(q{SELECT $1::bytea}, ['\x610062ff']);
$dbh->pg_send_prepare('pipeline_bytea', q{SELECT $1::bytea});
$dbh->pg_send_query_prepared('pipeline_bytea', ['\x610062ff']);
$dbh->pg_pipeline_sync();
for my $command (qw(params prepare prepared)) {
    $result = $dbh->pg_getresult();
    if ($command ne 'prepare') {
        is ($result->{rows}[0][0], '\x610062ff', "$command bytea preserves escaped NUL");
        ok (!utf8::is_utf8($result->{rows}[0][0]), "$command bytea is not flagged");
    }
    $dbh->pg_getresult();
}
$dbh->pg_getresult();
$dbh->pg_exit_pipeline_mode();
$dbh->do('DEALLOCATE pipeline_bytea');

# Field metadata is independent of the row count.
for my $condition ('true', 'false') {
    $dbh->pg_enter_pipeline_mode();
    $dbh->pg_send_query_params("SELECT 1::int4 AS id, 'hello'::text AS label WHERE $condition");
    $dbh->pg_pipeline_sync();
    $result = $dbh->pg_getresult();
    is_deeply ($result->{names}, ['id', 'label'], "Column names with condition $condition");
    is_deeply ($result->{types}, [23, 25], "Column type OIDs with condition $condition");
    $dbh->pg_getresult();
    $dbh->pg_getresult();
    $dbh->pg_exit_pipeline_mode();
}

# Error results retain SQLSTATE without raising away the result queue.
$dbh->do('CREATE TEMP TABLE dbd_pg_test_pipeline_unique (id integer PRIMARY KEY)');
$dbh->do('INSERT INTO dbd_pg_test_pipeline_unique VALUES (1)');
$dbh->pg_enter_pipeline_mode();
$dbh->pg_send_query_params('INSERT INTO dbd_pg_test_pipeline_unique VALUES (1)');
$dbh->pg_send_query_params('SELECT 42');
$dbh->pg_pipeline_sync();
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_FATAL_ERROR, 'Duplicate key is a fatal error result');
is ($result->{sqlstate}, '23505', 'Duplicate key SQLSTATE retained');
$dbh->pg_getresult();
$result = $dbh->pg_getresult();
is ($result->{status}, PGRES_PIPELINE_ABORTED, 'Command after error is skipped');
is ($result->{sqlstate}, undef, 'Skipped command has no server SQLSTATE');
$dbh->pg_getresult();
$result = $dbh->pg_getresult();
is ($result->{sqlstate}, undef, 'Sync marker has no server SQLSTATE');
$dbh->pg_exit_pipeline_mode();

# Exported result statuses have the libpq ABI values.
is_deeply ([PGRES_EMPTY_QUERY, PGRES_COMMAND_OK, PGRES_TUPLES_OK,
    PGRES_COPY_OUT, PGRES_COPY_IN, PGRES_BAD_RESPONSE, PGRES_NONFATAL_ERROR,
    PGRES_FATAL_ERROR, PGRES_COPY_BOTH, PGRES_SINGLE_TUPLE,
    PGRES_PIPELINE_SYNC, PGRES_PIPELINE_ABORTED], [0..11],
    'Pipeline export tag supplies result constants');

# Empty reads and commands beyond a flush boundary must not block.
$dbh->pg_enter_pipeline_mode();
for my $phase (qw(empty queued)) {
    $dbh->pg_send_query_params('SELECT 1') if $phase eq 'queued';
    eval {
        local $SIG{ALRM} = sub { die "pipeline read timed out\n"; };
        alarm 3;
        $dbh->pg_getresult();
    };
    alarm 0;
    like ($@, qr{requires a pending pipeline sync or flush request}, "$phase read rejected");
}
$dbh->pg_send_flush_request();
$dbh->pg_flush();
$dbh->pg_send_query_params('SELECT 2');
is ($dbh->pg_getresult()->{status}, PGRES_TUPLES_OK, 'Flush covers first command');
is ($dbh->pg_getresult(), undef, 'Flush includes first command separator');
eval {
    local $SIG{ALRM} = sub { die "pipeline read timed out\n"; };
    alarm 3;
    $dbh->pg_getresult();
};
alarm 0;
like ($@, qr{requires a pending pipeline sync or flush request}, 'Flush does not cover later sends');
$dbh->pg_pipeline_sync();
is_deeply ($dbh->pg_getresult()->{rows}, [['2']], 'Sync permits remaining command');
$dbh->pg_getresult();
is ($dbh->pg_getresult()->{status}, PGRES_PIPELINE_SYNC, 'Guard preserves sync result');
is ($dbh->pg_getresult(), undef, 'One final empty result after sync');
eval { $dbh->pg_getresult(); };
like ($@, qr{requires a pending pipeline sync or flush request}, 'Repeated empty read rejected');
$dbh->pg_exit_pipeline_mode();

cleanup_database($dbh,'test');
$dbh->disconnect;
