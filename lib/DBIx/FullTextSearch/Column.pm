
package DBIx::FullTextSearch::Column;
use strict;

# Open in the backend just sets the object
sub open {
	my ($class, $fts) = @_;
	return bless { 'fts' => $fts }, $class;
}

# Create creates the table(s) according to the parameters
sub _create_tables {
	my ($class, $fts) = @_;
	my $COUNT_FIELD = '';
	if ($fts->{'count_bits'}) {
		$COUNT_FIELD = "count $DBIx::FullTextSearch::BITS_TO_INT{$fts->{'count_bits'}} unsigned,"
	}
	my $CREATE_DATA = <<EOF;
		create table $fts->{'data_table'} (
			word_id $DBIx::FullTextSearch::BITS_TO_INT{$fts->{'word_id_bits'}} unsigned not null,
			doc_id $DBIx::FullTextSearch::BITS_TO_INT{$fts->{'doc_id_bits'}} unsigned not null,
			$COUNT_FIELD
			index (word_id),
			index (doc_id)
		)
EOF

	$fts->{'word_id_table'} = $fts->{'table'}.'_words'
				unless defined $fts->{'word_id_table'};
	
	
	my $CREATE_WORD_ID = <<EOF;
		create table $fts->{'word_id_table'} (
			word varchar($fts->{'word_length'}) binary
				default '' not null,
			id $DBIx::FullTextSearch::BITS_TO_INT{$fts->{'word_id_bits'}} unsigned not null auto_increment,
			primary key (id),
			unique (word)
			)
EOF

	my $dbh = $fts->{'dbh'};
        $dbh->do($CREATE_DATA) or return $dbh->errstr;
	push @{$fts->{'created_tables'}}, $fts->{'data_table'};
        $dbh->do($CREATE_WORD_ID) or return $dbh->errstr;
	push @{$fts->{'created_tables'}}, $fts->{'word_id_table'};
	return;
}

sub add_document {
	my ($self, $id, $words) = @_;
	my $fts = $self->{'fts'};
	my $dbh = $fts->{'dbh'};
	my $data_table = $fts->{'data_table'};
	my $word_id_table = $fts->{'word_id_table'};
	if (not defined $self->{'insert_wordid_sth'}) {
		$self->{'insert_wordid_sth'} = $dbh->prepare("
			insert into $word_id_table (word) values (?)
			");
		$self->{'insert_wordid_sth'}->{'PrintError'} = 0;
		$self->{'insert_wordid_sth'}->{'RaiseError'} = 0;
	}
	my $insert_wordid_sth = $self->{'insert_wordid_sth'};

	my $count_bits = $fts->{'count_bits'};
	my $insert_worddoc_sth = ( defined $self->{'insert_worddoc_sth'}
		? $self->{'insert_worddoc_sth'}
		: $self->{'insert_worddoc_sth'} = (
			$count_bits
			? $dbh->prepare("
				insert into $data_table
				select id, ?, ? from $word_id_table
					where word = ?")
			: $dbh->prepare("
				insert into $data_table
				select id, ? from $word_id_table
					where word = ?")
			) );
	my $num_words = 0;
	for my $word ( keys %$words ) {
		$insert_wordid_sth->execute($word);
		if ($count_bits) {
			$insert_worddoc_sth->execute($id, $words->{$word}, $word);
		}
		else {
			$insert_worddoc_sth->execute($id, $word);
		}
		$num_words += $words->{$word};
	}
	return $num_words;
}

sub delete_document {
	my $self = shift;
	my $fts = $self->{'fts'};
	my $dbh = $fts->{'dbh'};
	my $data_table = $fts->{'data_table'};
	my $sth = $dbh->prepare("delete from $data_table where doc_id = ?");
	for my $id (@_) { $sth->execute($id); }
}

sub update_document {
	my ($self, $id, $words) = @_;
	$self->delete_document($id);
	$self->add_document($id, $words);
}

sub contains_hashref {
	my $self = shift;
	my $fts = $self->{'fts'};
	my $dbh = $fts->{'dbh'};
	my $data_table = $fts->{'data_table'};
	my $word_id_table = $fts->{'word_id_table'};

	my $count_bits = $fts->{'count_bits'};
	my $sth = ( defined $self->{'get_data_sth'}
		? $self->{'get_data_sth'}
		: ( $count_bits
		? ( $self->{'get_data_sth'} = $dbh->prepare(
			"select doc_id, count
			from $data_table, $word_id_table
			where word like ?
				and id = word_id" ) )
		: ( $self->{'get_data_sth'} = $dbh->prepare(
			"select doc_id, 1
			from $data_table, $word_id_table
			where word like ?
				and id = word_id" ) )
			) );

	my $out = {};
	for my $word (@_) {
		$sth->execute($word);
		while (my ($doc, $count) = $sth->fetchrow_array) {
			$out->{$doc} += $count;
		}
		$sth->finish;
	}
        $out;
}

sub common_word {
        my ($self, $k) = @_;
        my $fts = $self->{'fts'};
        my $dbh = $fts->{'dbh'};

        my $num = $fts->document_count;

        $k /= 100;

        my $SQL = <<EOF;
                select word_id, count(*)/? as k
                from $fts->{'data_table'}
                group by word_id
                having k >= ?
EOF
        my $ary_ref = $dbh->selectcol_arrayref($SQL, {}, $num, $k);
        return unless @$ary_ref;

        my $QUESTION_MARKS = join ',', ('?') x scalar(@$ary_ref);

        $SQL = <<EOF;
                select word
                from $fts->{'word_id_table'}
                where id IN ($QUESTION_MARKS)
EOF
        return $dbh->selectcol_arrayref($SQL, {}, @$ary_ref);
}

*parse_and_index_data = \&DBIx::FullTextSearch::parse_and_index_data_count;

1;

