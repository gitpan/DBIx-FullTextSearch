
use strict;
use ExtUtils::MakeMaker;
use Data::Dumper;

# this is to catch values from previous perl Makefile.PL run
use lib 'testconfig';

# we try to load the previous config values
eval 'use DBIx::FullTextSearch::TestConfig';

# if we got some values and we are not forced to run the dialog again
# with -s or -setup option, just use those defaults, but say it
if (defined $DBIx::FullTextSearch::TestConfig::Config{'dsn'}
	and not (@ARGV and ($ARGV[0] eq '-s' or $ARGV[0] eq '-setup'))) {
	print <<'EOF';
	For the test suite, we use the database and user info specified
	during the previous run. If you want to change the values, run
	perl Makefile.PL -s.
EOF
	}
# we will ask the questions
else {
	print <<'EOF';
	We will ask you for the database info to run the test. To enter
	undefined value, accept empty string or say undef.
EOF
	if (not defined $DBIx::FullTextSearch::TestConfig::Config{'dsn'}) {
		$DBIx::FullTextSearch::TestConfig::Config{'dsn'} = 'dbi:mysql:test';
		}
	$DBIx::FullTextSearch::TestConfig::Config{'dsn'}
		= prompt 'The dsn for tests:', $DBIx::FullTextSearch::TestConfig::Config{'dsn'};
	$DBIx::FullTextSearch::TestConfig::Config{'user'}
		= prompt 'The username:', $DBIx::FullTextSearch::TestConfig::Config{'user'};
	$DBIx::FullTextSearch::TestConfig::Config{'password'}
		= prompt 'The password:', $DBIx::FullTextSearch::TestConfig::Config{'password'};

	for (qw! dsn user password !) {
		if ($DBIx::FullTextSearch::TestConfig::Config{$_} eq ''
			or $DBIx::FullTextSearch::TestConfig::Config{$_} eq 'undef') {
			$DBIx::FullTextSearch::TestConfig::Config{$_} = undef;
			}
		}
	}

# create the testconfig directory for the DBIx::FullTextSearch::TestConfig.pm file
mkdir 'testconfig', 0700;
mkdir 'testconfig/DBIx', 0700;
mkdir 'testconfig/DBIx/FullTextSearch', 0700;

# write out the TestConfig file, to be used either for next perl
# Makefile.PL in this session, or (after install) even for further
# versions of this module
open OUT, '> testconfig/DBIx/FullTextSearch/TestConfig.pm' or die "Error writing the TestConfig\n";
print OUT Data::Dumper->new([ \%DBIx::FullTextSearch::TestConfig::Config ],
	[ '*DBIx::FullTextSearch::TestConfig::Config' ])->Dump,
	"1;\n";
close OUT;

WriteMakefile(
	NAME =>		'DBIx::FullTextSearch',
	VERSION_FROM =>	'FullTextSearch.pm',
	PM =>		{ 'FullTextSearch.pm' => '$(INST_LIBDIR)/FullTextSearch.pm',
			'String.pm' => '$(INST_LIBDIR)/FullTextSearch/String.pm',
			'File.pm' => '$(INST_LIBDIR)/FullTextSearch/File.pm',
			'URL.pm' => '$(INST_LIBDIR)/FullTextSearch/URL.pm',
			'Blob.pm' => '$(INST_LIBDIR)/FullTextSearch/Blob.pm',
			'Column.pm' => '$(INST_LIBDIR)/FullTextSearch/Column.pm',
			'Phrase.pm' => '$(INST_LIBDIR)/FullTextSearch/Phrase.pm',
			'Table.pm' => '$(INST_LIBDIR)/FullTextSearch/Table.pm',
			'testconfig/DBIx/FullTextSearch/TestConfig.pm' => '$(INST_LIBDIR)/FullTextSearch/TestConfig.pm',
			},
	MAN1PODS =>	{ 'mycontextadmin' => '$(INST_MAN1DIR)/mycontextadmin.1' },
	dist =>		{ COMPRESS => 'gzip -9f', SUFFIX => 'gz',
			POSTOP => 'mv $(DISTNAME)-$(VERSION).tar.gz ../' },
	macro =>	{ BENCH_FILES => 'bench/*.t' },
	depend =>	{ bench => q!pure_all
		for i in $(BENCH_FILES) ; do echo $$i ; $(FULLPERL) -I$(INST_ARCHLIB) -I$(INST_LIB) $$i ; done
		! },
	);

