<?php
$pdo = getPDO();

$method = $_SERVER['REQUEST_METHOD'];
$requestUri = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
$basePath = '/Google_Scholar_Server_v2';
$path = trim(str_replace($basePath, '', $requestUri), '/');
$segments = explode('/', $path);


if ($segments[1] === 'all') {
    try {
        $stmt = $pdo->query("CALL get_all_departments()");
        $departments = $stmt->fetchAll();
        while ($stmt->nextRowset()) {;} // Καθαρισμός για procedure με multiple result sets

        echo json_encode($departments, JSON_UNESCAPED_UNICODE);
    } catch (Exception $e) {
        http_response_code(500);
        echo json_encode(['error' => 'Server error: ' . $e->getMessage()]);
    }
} elseif ($segments[1] === 'statistics') {
    $departments = null;
    $roles = null;

    if ($segments[2] === 'token') {
        if (isset($_GET['departments']) && isset($_GET['roles'])) {
            $departments = decode_10bit_gzip_base64url($_GET['departments']);
            $roles = decode_10bit_gzip_base64url($_GET['roles']);
        }
    } elseif ($segments[2] === 'ids') {
        if (isset($_GET['departments']) && isset($_GET['roles'])) {
            $departments = array_map('intval', explode(',', $_GET['departments']));
            $roles = array_map('intval', explode(',', $_GET['roles']));
        }
    } else {
        if (isset($_GET['departments']) && isset($_GET['roles'])) {
            $departments = array_map('intval', explode(',', $_GET['departments']));
            $roles = array_map('intval', explode(',', $_GET['roles']));
        }
    }

    if (!$departments || !$roles || !is_array($departments) || !is_array($roles)) {
        http_response_code(400);
        echo json_encode(["error" => "Missing or invalid 'departments' or 'roles'"]);
        exit;
    }

    $department_ids = implode(',', $departments);
    $role_ids = implode(',', $roles);

    try {
        $stmt = $pdo->prepare("CALL get_department_stats_by_depts_and_roles(:dept_ids, :role_ids)");
        $stmt->bindParam(':dept_ids', $department_ids);
        $stmt->bindParam(':role_ids', $role_ids);
        $stmt->execute();

        $results = $stmt->fetchAll(PDO::FETCH_ASSOC);
        while ($stmt->nextRowset()) {;}

        echo json_encode($results, JSON_UNESCAPED_UNICODE);
    } catch (PDOException $e) {
        http_response_code(500);
        echo json_encode(["error" => "Database error: " . $e->getMessage()]);
    }

    exit;
} else {
    http_response_code(405);
    echo json_encode(['error' => 'Method not allowed']);
}
