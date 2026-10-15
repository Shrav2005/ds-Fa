#!/usr/bin/env perl
use strict;
use warnings;

sub utf8_bytes {
    my ($codepoint) = @_;
    return pack('C', $codepoint) if $codepoint <= 0x7f;
    return pack('C2', 0xc0 | ($codepoint >> 6), 0x80 | ($codepoint & 0x3f))
        if $codepoint <= 0x7ff;
    return pack('C3', 0xe0 | ($codepoint >> 12), 0x80 | (($codepoint >> 6) & 0x3f), 0x80 | ($codepoint & 0x3f))
        if $codepoint <= 0xffff;
    return pack('C4', 0xf0 | ($codepoint >> 18), 0x80 | (($codepoint >> 12) & 0x3f), 0x80 | (($codepoint >> 6) & 0x3f), 0x80 | ($codepoint & 0x3f));
}

sub json_string {
    my ($line, $key) = @_;
    my $quoted_key = quotemeta($key);
    return undef unless $line =~ /"$quoted_key"\s*:\s*"((?:\\.|[^"\\])*)"/;

    my $value = $1;
    $value =~ s/\\(["\\\/])/$1/g;
    $value =~ s/\\b/\x08/g;
    $value =~ s/\\f/\x0c/g;
    $value =~ s/\\n/\n/g;
    $value =~ s/\\r/\r/g;
    $value =~ s/\\t/\t/g;
    $value =~ s/\\u(d[89ab][0-9a-f]{2})\\u(d[c-f][0-9a-f]{2})/
        utf8_bytes(0x10000 + ((hex($1) - 0xd800) << 10) + hex($2) - 0xdc00)/egi;
    $value =~ s/\\u([0-9a-fA-F]{4})/utf8_bytes(hex($1))/eg;
    return $value;
}

sub emit {
    my ($key, $value) = @_;
    print "$key\t$value\n";
}

while (my $line = <STDIN>) {
    my $record_type = json_string($line, 'record_type') // '';

    if ($record_type eq 'request') {
        my $bed_type = json_string($line, 'bed_type');
        my $hospital_name = json_string($line, 'hospital_name');
        my $created_at = json_string($line, 'created_at');
        emit("requests_by_bed_type|$bed_type", 1) if $bed_type;
        emit("requests_by_hospital|$hospital_name", 1) if $hospital_name;

        if ($created_at) {
            my ($hour, $zone, $sign, $zone_hour, $zone_minute) =
                $created_at =~ /T(\d{2}):\d{2}:\d{2}(?:\.\d+)?(Z|([+-])(\d{2}):(\d{2}))\z/;
            die "Request analytics timestamps must include a valid timezone.\n" unless defined $hour;
            my $offset = $zone eq 'Z' ? 0 : ($zone_hour * 60 + $zone_minute) * ($sign eq '+' ? 1 : -1);
            my $utc_minutes = ($hour * 60 - $offset) % 1440;
            $utc_minutes += 1440 if $utc_minutes < 0;
            emit(sprintf('requests_by_hour|%02d:00', int($utc_minutes / 60)), 1);
        }
    } elsif ($record_type eq 'reservation') {
        my $hospital_name = json_string($line, 'hospital_name');
        my $bed_type = json_string($line, 'bed_type');
        emit("reservations_by_hospital|$hospital_name", 1) if $hospital_name;
        emit("reservations_by_bed_type|$bed_type", 1) if $bed_type;
        emit('total_reservations', 1);
    } else {
        die "Unsupported analytics record type: $record_type\n";
    }
}
