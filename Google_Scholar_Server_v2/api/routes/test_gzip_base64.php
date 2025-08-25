<?php
require_once 'functions.php';
function base64url_encode(string $data): string {
    return rtrim(strtr(base64_encode($data), '+/', '-_'), '=');
}

function encode_10bit_gzip_base64url(array $numbers): string {
    $bitstring = '';
    foreach ($numbers as $n) {
        if ($n < 0 || $n > 1023) {
            throw new Exception("Number $n out of 10-bit range");
        }
        $bitstring .= str_pad(decbin($n), 10, '0', STR_PAD_LEFT);
    }

    $binary = '';
    for ($i = 0; $i < strlen($bitstring); $i += 8) {
        $byte = substr($bitstring, $i, 8);
        $byte = str_pad($byte, 8, '0', STR_PAD_RIGHT);
        $binary .= chr(bindec($byte));
    }

    $gzipped = gzcompress($binary, 9);
    return base64url_encode($gzipped);
}

$original_numbers = [76];

$encoded = encode_10bit_gzip_base64url($original_numbers);
$decoded = decode_10bit_gzip_base64url($encoded);

$url_length = strlen(urlencode($encoded));

echo "Numbers: " . count($original_numbers) . "\n";
echo "Encoded string length: " . strlen($encoded) . " chars\n";
echo "URL safe: " . ($url_length <= 1990 ? "YES" : "NO — Too long") . "\n\n";
echo "Encoded string: \n" . urlencode($encoded) . " \n";

