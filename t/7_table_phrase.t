
use strict;
use vars qw! $dbh !;

require 't/test.lib';

print "1..1\n";

use DBIx::FullTextSearch;

drop_all_tables();

$dbh->do(qq{
CREATE TABLE _fts_test_table_phrase_src (
  UID int(20) NOT NULL,
  Text mediumtext,
  PRIMARY KEY (UID)
)});

eval {
  # Create a table FTS with backend phrase.
  my $fts = DBIx::FullTextSearch->create($dbh, '_fts_test_table_phrase',
        frontend => 'table', backend => 'phrase',
        table_name => '_fts_test_table_phrase_src', column_name => 'Text',
        column_id_name => 'UID');
};

if ($@){
  print "not ok 1 # $@\n";
} else {
  print "ok 1\n";
}

drop_all_tables();

sub drop_all_tables {
	for my $tableref (@{$dbh->selectall_arrayref('show tables')}) {
		next unless $tableref->[0] =~ /^_fts_test/;
		print "Dropping $tableref->[0]\n";
		$dbh->do("drop table $tableref->[0]");
		}
	}
