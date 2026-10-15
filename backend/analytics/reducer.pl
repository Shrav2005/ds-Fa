#!/usr/bin/env perl
use strict;
use warnings;

my ($current_key, $current_total);

while (my $line = <STDIN>) {
    chomp $line;
    my ($key, $value) = split /\t/, $line, 2;
    die "Malformed Hadoop Streaming value.\n"
        unless defined $key && defined $value && $value =~ /\A\d+\z/;

    if (defined $current_key && $key eq $current_key) {
        $current_total += $value;
        next;
    }

    print "$current_key\t$current_total\n" if defined $current_key;
    $current_key = $key;
    $current_total = $value;
}

print "$current_key\t$current_total\n" if defined $current_key;
