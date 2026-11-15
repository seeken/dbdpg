#!perl
use 5.008001;
use strict;
use warnings;
use lib 'blib/lib', 'blib/arch', 't';
use Test::More;
use DBD::Pg qw(:pg_transaction PG_ASYNC);
require 'dbdpg_test_setup.pl';
my $dbh = connect_database({AutoCommit => 1});
plan skip_all => 'Connection to database failed' unless $dbh;
$dbh->{PrintError} = 0;
$dbh->{RaiseError} = 1;
my $supported = $dbh->{pg_pipeline_supported};
ok(defined($supported), 'capability is defined even on old libpq');
is(!!$dbh->pg_can_pipeline, !!$supported, 'default capability matches build and loaded libpq');
ok($dbh->{pg_pipeline}, 'pipeline enabled by default');
eval { $dbh->{pg_pipeline_supported} = !$supported };
like($@, qr/read-only/, 'raw capability cannot be overridden');
is(!!$dbh->{pg_pipeline_supported}, !!$supported, 'capability unchanged');
is($dbh->pg_transaction_status, PG_TRANS_IDLE, 'local status always available');
$dbh->do('BEGIN');
is($dbh->pg_transaction_status, PG_TRANS_INTRANS, 'raw transaction detected');
eval { $dbh->do('SELECT 1/0') };
is($dbh->pg_transaction_status, PG_TRANS_INERROR, 'failed transaction detected');
$dbh->do('ROLLBACK');
$dbh->do('SELECT 1', {pg_async => PG_ASYNC});
is($dbh->pg_transaction_status, PG_TRANS_ACTIVE, 'pending async detected');
$dbh->pg_result;
$dbh->{pg_pipeline} = 0;
ok(!$dbh->pg_can_pipeline, 'handle opt-out disables effective capability');
is(!!$dbh->{pg_pipeline_supported}, !!$supported, 'opt-out preserves raw capability');
eval { $dbh->pg_pipeline([{sql => 'SELECT 1/0'}]) };
like($@, qr/pg_pipeline (?:disabled|unsupported)/, 'explicit unavailable call raises clearly');
is($dbh->state, '0A000', 'unavailable feature SQLSTATE');
is($dbh->pg_transaction_status, PG_TRANS_IDLE, 'refused frame sends no transaction');
is($dbh->selectrow_array('SELECT 42'), 42, 'sequential execution still works');
$dbh->{pg_pipeline} = 1;
is(!!$dbh->pg_can_pipeline, !!$supported, 'reenabling respects build and runtime');
if ($supported) {
    is($dbh->pg_pipeline([{sql => 'SELECT 7'}])->[0]{rows}[0][0], 7, 'reenabled frame works');
}
else {
    eval { $dbh->pg_pipeline([{sql => 'SELECT 1'}]) };
    like($@, qr/unsupported.*libpq 14/, 'unsupported build raises with minimum library version');
    local $dbh->{RaiseError} = 0;
    ok(!defined $dbh->pg_pipeline([{sql => 'SELECT 1'}]), 'unsupported build returns undef without RaiseError');
    is($dbh->state, '0A000', 'unsupported build retains feature SQLSTATE');
}
my $off = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
    {AutoCommit => 1, RaiseError => 1, PrintError => 0, pg_pipeline => 0});
ok(!$off->{pg_pipeline}, 'connect-time opt-out stored');
ok(!$off->pg_can_pipeline, 'connect-time opt-out disables capability');
is($off->selectrow_array('SELECT 43'), 43, 'connect-time opt-out supports ordinary DBI');
{
    local $off->{RaiseError} = 0;
    ok(!defined $off->pg_pipeline([{sql => 'BEGIN'}]), 'disabled call returns undef without RaiseError');
    like($off->errstr, qr/pg_pipeline (?:disabled|unsupported)/, 'disabled call sets clear error');
    is($off->state, '0A000', 'disabled call sets SQLSTATE');
    is($off->pg_transaction_status, PG_TRANS_IDLE, 'disabled call sends no BEGIN');
}
$off->disconnect;
$dbh->disconnect;
is($dbh->pg_transaction_status, PG_TRANS_UNKNOWN, 'disconnected status always available');
done_testing;
