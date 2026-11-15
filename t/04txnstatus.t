#!perl
# -*-cperl-*-

use 5.008001;
use strict;
use warnings;
use lib 'blib/lib', 'blib/arch', 't';
use Test::More;
use DBD::Pg qw(:pg_transaction PG_ASYNC);
require 'dbdpg_test_setup.pl';

my $dbh = connect_database({AutoCommit => 1});
plan skip_all => 'Connection to database failed' unless $dbh;
$dbh->{RaiseError} = 1;
$dbh->{PrintError} = 0;
is_deeply ([PG_TRANS_IDLE, PG_TRANS_ACTIVE, PG_TRANS_INTRANS,
           PG_TRANS_INERROR, PG_TRANS_UNKNOWN], [0, 1, 2, 3, 4],
          'transaction constants match libpq');
is ($dbh->pg_transaction_status, PG_TRANS_IDLE, 'idle handle');
is ($dbh->{pg_transaction_status}, PG_TRANS_IDLE, 'read-only status attribute');
eval { $dbh->{pg_transaction_status} = PG_TRANS_INTRANS };
like ($@, qr/read-only/, 'status cannot be overridden');
$dbh->do('BEGIN');
is ($dbh->pg_transaction_status, PG_TRANS_INTRANS, 'raw BEGIN');
eval { $dbh->do('SELECT 1/0') };
is ($dbh->pg_transaction_status, PG_TRANS_INERROR, 'failed transaction');
$dbh->do('ROLLBACK');
$dbh->do('SELECT 17', {pg_async => PG_ASYNC});
is ($dbh->pg_transaction_status, PG_TRANS_ACTIVE, 'pending async result');
is ($dbh->{pg_transaction_status}, PG_TRANS_ACTIVE, 'attribute does not consume result');
$dbh->pg_result;
is ($dbh->pg_transaction_status, PG_TRANS_IDLE, 'async result consumed normally');
$dbh->begin_work;
is ($dbh->pg_transaction_status, PG_TRANS_IDLE, 'begin_work is deferred');
$dbh->do('SELECT 1');
is ($dbh->pg_transaction_status, PG_TRANS_INTRANS, 'ordinary execute starts transaction');
$dbh->rollback;
$dbh->disconnect;
is ($dbh->pg_transaction_status, PG_TRANS_UNKNOWN, 'disconnected method');
is ($dbh->{pg_transaction_status}, PG_TRANS_UNKNOWN, 'disconnected attribute');
done_testing;
