<?php
$pdo = getPDO();
$method = $_SERVER['REQUEST_METHOD'];
$requestUri = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
$basePath = '/Google_Scholar_Server_v2';
$path = trim(str_replace($basePath, '', $requestUri), '/');
$segments = explode('/', $path);

if ($segments[0] === 'roles') {
    try {
        $stmt = $pdo->query("CALL get_all_roles()");
        $roles = $stmt->fetchAll();
        while ($stmt->nextRowset()) {;}
        echo json_encode($roles, JSON_UNESCAPED_UNICODE);
    } catch (Exception $e) {
        http_response_code(500);
        echo json_encode(['error' => 'Server error: ' . $e->getMessage()]);
    }
} else {
    http_response_code(405);
    echo json_encode(['error' => 'Method not allowed']);
}
