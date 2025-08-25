<?php
function base64url_decode(string $data): string {
    $data = strtr($data, '-_', '+/');
    $padding = strlen($data) % 4;
    if ($padding > 0) {
        $data .= str_repeat('=', 4 - $padding);
    }
    return base64_decode($data);
}

function decode_10bit_gzip_base64url(string $encoded): array {
    $binary = gzuncompress(base64url_decode($encoded));
    $bitstring = '';
    for ($i = 0; $i < strlen($binary); $i++) {
        $bitstring .= str_pad(decbin(ord($binary[$i])), 8, '0', STR_PAD_LEFT);
    }

    $numbers = [];
    for ($i = 0; $i + 10 <= strlen($bitstring); $i += 10) {
        $chunk = substr($bitstring, $i, 10);
        $numbers[] = bindec($chunk);
    }

    return $numbers;
}

function loadEnv($path) {
    if (!file_exists($path)) return;

    $lines = file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    foreach ($lines as $line) {
        if (strpos(trim($line), '#') === 0) continue;
        list($name, $value) = explode('=', $line, 2);
        putenv(trim($name) . '=' . trim($value));
    }
}