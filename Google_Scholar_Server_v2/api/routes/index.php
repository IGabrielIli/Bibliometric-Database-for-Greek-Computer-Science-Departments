<?php
header('Content-Type: application/json');
require_once __DIR__ . '/../config/db.php';
require_once 'functions.php';

$requestUri = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
$basePath = '/Google_Scholar_Server_v2';
$path = trim(str_replace($basePath, '', $requestUri), '/');
$segments = explode('/', $path);
$method = $_SERVER['REQUEST_METHOD'];

if ($method === 'GET') {
    switch ($segments[0]) {
        case 'departments':
            require __DIR__ . '/departments.php';
            break;

        case 'roles':
            require __DIR__ . '/roles.php';
            break;

        case 'staff':
            require __DIR__ . '/staff.php';
            break;

        default:
            http_response_code(404);
            header("Content-Type: application/json");
            echo json_encode(["error" => "Endpoint not found"]);
            break;
    }
} else {
    http_response_code(405);
    header("Content-Type: application/json");
    echo json_encode(["error" => "Method not allowed"]);
}