#!perl
use 5.008001;
use strict;
use warnings;
use utf8;
use B ();
use lib 'blib/lib', 'blib/arch', 't';
use Test::More;
use DBD::Pg qw(:pg_transaction :pg_types);
require 'dbdpg_test_setup.pl';
my $dbh = connect_database({AutoCommit => 1});
plan skip_all => 'Connection to database failed' unless $dbh;
plan skip_all => 'libpq pipeline support unavailable' unless $dbh->pg_can_pipeline;
$dbh->{PrintError} = 0;
$dbh->{RaiseError} = 1;
$dbh->do("SET client_encoding='UTF8'");
$dbh->{pg_enable_utf8} = -1;
is($dbh->pg_transaction_status, PG_TRANS_IDLE, 'local idle state');
$dbh->do('BEGIN');
is($dbh->pg_transaction_status, PG_TRANS_INTRANS, 'raw BEGIN detected');
eval {$dbh->do('SELECT 1/0')};
is($dbh->pg_transaction_status, PG_TRANS_INERROR, 'failed host transaction detected');
$dbh->do('ROLLBACK');
$dbh->do('SELECT pg_sleep(0.01)', {pg_async => PG_ASYNC});
is($dbh->pg_transaction_status, PG_TRANS_ACTIVE, 'active async result detected locally');
eval {$dbh->pg_pipeline([{sql=>'SELECT 1'}])};
like($@, qr/without COPY or async/, 'pending async result is refused');
$dbh->pg_result;
is($dbh->selectrow_array('SELECT 7'), 7, 'pending result remains usable');

sub frame {
    return $dbh->pg_pipeline([{sql=>'BEGIN READ ONLY NOT DEFERRABLE'},
        {sql=>$_[0],params=>$_[1]||[],types=>$_[2]||[],sync=>1},{sql=>'ROLLBACK'}]);
}
my $sql = q{SELECT ?::text AS "é", ?::bytea AS bytes, ?::integer AS n,
    ?::boolean AS b, ?::int[] AS a, ?::numeric AS d, ?::text AS nil};
my @values = ('héllo', "\0\xff\\", 42, 1, [1,undef,3], '12.340', undef);
my @types = ({pg_type=>PG_TEXT}, {pg_type=>PG_BYTEA}, {pg_type=>PG_INT4},
    {pg_type=>PG_BOOL}, undef, undef, undef);
sub value_flags {
    return B::svref_2object(\$_[0])->FLAGS & (B::SVf_IOK()|B::SVp_IOK()|B::SVf_NOK()|B::SVp_NOK()|B::SVf_POK()|B::SVp_POK()|B::SVf_UTF8());
}
for my $utf8 (-1,0,1) {
    for my $bool (0,1) {
        $dbh->{pg_enable_utf8}=$utf8; $dbh->{pg_bool_tf}=$bool;
        my $mode_sql=$sql;
        my @mode_values=@values;
        if ($utf8 == 0) { utf8::encode($mode_sql); utf8::encode($mode_values[0]); }
        my $sth=$dbh->prepare($mode_sql,{pg_switch_prepared=>0});
        $sth->bind_param($_+1,$mode_values[$_],$types[$_]) for 0..$#values;
        $sth->execute;
        my $expected=$sth->fetchall_arrayref;
        my $result=frame($mode_sql,\@mode_values,\@types);
        is_deeply([map {value_flags($_)} @{$result->[1]{rows}[0]}],
            [map {value_flags($_)} @{$expected->[0]}], 'scalar representation flags match normal fetch');
        is_deeply($result->[1]{rows},$expected,"normal conversion utf8=$utf8 bool=$bool");
        is_deeply($result->[1]{columns},$sth->{NAME},'normal column names');
        is_deeply($result->[1]{types},$sth->{pg_type},'normal column types');
        is(utf8::is_utf8($result->[1]{rows}[0][0]),utf8::is_utf8($expected->[0][0]),'text UTF8 flag matches');
        ok(!utf8::is_utf8($result->[1]{rows}[0][1]),'bytea stays bytes');
        is($dbh->pg_transaction_status,PG_TRANS_IDLE,'frame leaves idle');
    }
}
for my $mode (-1,0,1) {
    local $dbh->{RaiseError}=0;
    $dbh->{pg_enable_utf8}=$mode;
    $dbh->do('SELECT 1/0');
    my $expected_flag=utf8::is_utf8($dbh->errstr);
    my $error_frame=frame('SELECT 1/0');
    is(utf8::is_utf8($error_frame->[1]{error}),$expected_flag,'per-command error UTF8 flag matches normal execution');
    is(utf8::is_utf8($dbh->errstr),$expected_flag,'DBI error UTF8 flag matches normal execution');
}
{
    local $dbh->{RaiseError}=0;
    my $res=$dbh->pg_pipeline([{sql=>'BEGIN'}, {sql=>'SELECT 1/0'},
        {sql=>'SELECT 99',sync=>1}, {sql=>'ROLLBACK'}]);
    is($res->[1]{state},'22012','per-command SQLSTATE');
    is($res->[2]{status},'PGRES_PIPELINE_ABORTED','commands after error skipped through sync');
    is($res->[3]{status},'PGRES_COMMAND_OK','rollback after sync executes');
    is($dbh->state,'22012','DBI SQLSTATE is first error');
    is($dbh->pg_error_field('sqlstate'),'22012','first error fields survive cleanup and statement destruction');
    is($dbh->pg_transaction_status,PG_TRANS_IDLE,'error frame cleaned up');
}
eval {frame('SELECT 1/0')};
like($@,qr/division by zero/,'RaiseError reports original error after draining');
is($dbh->pg_transaction_status,PG_TRANS_IDLE,'RaiseError occurs after cleanup');
is($dbh->selectrow_array('SELECT 9'),9,'ordinary execution after pipeline error');
ok(!defined($dbh->pg_error_field('sqlstate')),'later successful execution replaces error fields');

$dbh->begin_work;
my $r=$dbh->pg_pipeline([{sql=>'SELECT 1'}]);
ok(!$dbh->{AutoCommit},'deferred begin_work stays active');
is($dbh->pg_transaction_status,PG_TRANS_INTRANS,'pipeline starts deferred transaction');
$dbh->pg_pipeline([{sql=>'ROLLBACK'}]);
ok($dbh->{AutoCommit},'explicit rollback restores begin_work AutoCommit');
ok(!$dbh->{BegunWork},'BegunWork cleared');
$dbh->{AutoCommit}=0;
$dbh->pg_pipeline([{sql=>'SELECT 1'}, {sql=>'ROLLBACK'}]);
ok(!$dbh->{AutoCommit},'persistent AutoCommit off remains off');
is($dbh->pg_transaction_status,PG_TRANS_IDLE,'persistent mode rollback is idle');
$dbh->{AutoCommit}=1;
$dbh->do('BEGIN');
$dbh->pg_pipeline([{sql=>'SELECT 1'}]);
is($dbh->pg_transaction_status,PG_TRANS_INTRANS,'raw host transaction preserved');
$dbh->do('ROLLBACK');

# Exercise both socket directions beyond their buffers without a deadlock.
$r=$dbh->pg_pipeline([{sql=>q{SELECT repeat('a', 2000000)}},
    {sql=>'SELECT length(?::text)',params=>['b' x 2000000]}]);
is(length($r->[0]{rows}[0][0]),2000000,'large first result');
is($r->[1]{rows}[0][0],2000000,'large later parameter');
for my $bad ([{sql=>'SELECT ?',params=>[]}], [{sql=>"SELECT 1\0;SELECT 2"}], []) {
    eval {$dbh->pg_pipeline($bad)};
    ok($@,'malformed frame rejected before send');
    is($dbh->pg_transaction_status,PG_TRANS_IDLE,'malformed frame leaves idle');
}
$dbh->do("SET client_min_messages=notice");
$dbh->{Warn}=1; $dbh->{PrintWarn}=1;
$dbh->do(q{CREATE OR REPLACE FUNCTION pg_temp.pipeline_notice() RETURNS int LANGUAGE plpgsql AS $$
 BEGIN RAISE NOTICE 'pipeline notice'; RETURN 1; END $$});
{
    local $SIG{__WARN__}=sub {die "fatal warning callback\n"};
    eval {frame('SELECT pg_temp.pipeline_notice()')};
    like($@,qr/fatal warning callback/,'fatal warning callback propagated after draining');
}
is($dbh->pg_transaction_status,PG_TRANS_IDLE,'fatal warning leaves frame ended');
is($dbh->selectrow_array('SELECT 11'),11,'handle usable after fatal warning');
eval {$dbh->pg_pipeline([{sql=>'BEGIN'}, {sql=>'/* outer /* nested */ */ COMMIT'}, {sql=>'SELECT 1'}])};
like($@,qr/Transaction completion must/,'multi-transaction frame refused before dispatch');
is($dbh->pg_transaction_status,PG_TRANS_IDLE,'rejected multi-transaction frame leaves idle');
$dbh->do('BEGIN');
$dbh->pg_pipeline([{sql=>'SAVEPOINT pipe_sp'}, {sql=>'ROLLBACK /* comment */ TO pipe_sp'}, {sql=>'SELECT 1'}]);
is($dbh->pg_transaction_status,PG_TRANS_INTRANS,'rollback to savepoint preserves transaction');
$dbh->do('ROLLBACK');
my $copy=connect_database({AutoCommit=>1});
$copy->{PrintError}=0;
eval {$copy->pg_pipeline([{sql=>'COPY (SELECT 1) TO STDOUT'}])};
like($@,qr/COPY is not supported/,'COPY rejected without leaving a busy connection');
ok(!$copy->{Active},'unsupported COPY disconnects');
is($copy->pg_transaction_status,PG_TRANS_UNKNOWN,'disconnected local status is UNKNOWN');
{
    local $dbh->{RaiseError}=0;
    my $empty=$dbh->pg_pipeline([{sql=>'/* no statement */'}]);
    ok($empty->[0]{error},'empty command has a per-command error');
}
my $victim=connect_database({AutoCommit=>1});
$victim->{RaiseError}=0; $victim->{PrintError}=0;
$dbh->do('SELECT pg_terminate_backend(?)',undef,$victim->{pg_pid});
my $lost=$victim->pg_pipeline([{sql=>'SELECT 1'}]);
ok(!defined($lost),'connection loss returns undef without RaiseError');
ok(!$victim->{Active},'connection loss closes uncertain handle');
is($victim->pg_transaction_status,PG_TRANS_UNKNOWN,'lost connection local state is UNKNOWN');
SKIP: {
    skip 'POSIX descriptor test', 2 if $^O eq 'MSWin32';
    my @files;
    for (1..1050) {
        open my $fh, '<', '/dev/null' or last;
        push @files, $fh;
    }
    skip 'process descriptor limit is too low', 2 unless @files == 1050;
    my $high=DBI->connect($ENV{DBI_DSN},$ENV{DBI_USER},$ENV{DBI_PASS},
        {RaiseError=>1,PrintError=>0,AutoCommit=>1});
    cmp_ok($high->{pg_socket},'>=',1024,'connection above select descriptor limit');
    is($high->pg_pipeline([{sql=>'SELECT 23'}])->[0]{rows}[0][0],23,'pipeline services a high descriptor');
    $high->disconnect;
}
$dbh->disconnect;
is($dbh->pg_transaction_status,PG_TRANS_UNKNOWN,'disconnected status safe');
done_testing;
