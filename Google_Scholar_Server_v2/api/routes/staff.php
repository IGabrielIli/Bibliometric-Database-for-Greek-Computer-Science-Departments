<?php
$pdo = getPDO();
$method = $_SERVER['REQUEST_METHOD'];
$requestUri = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
$basePath = '/Google_Scholar_Server_v2';
$path = trim(str_replace($basePath, '', $requestUri), '/');
$segments = explode('/', $path);

if ($segments[1] === 'overall') {
    $staff_ids = null;

    if (isset($_GET['ids'])) {
        $staff_ids = array_map('intval', explode(',', $_GET['ids']));
    } elseif (isset($_GET['token'])) {
        $staff_ids = decode_10bit_gzip_base64url($_GET['token']);
    }

    if (!$staff_ids || !is_array($staff_ids) || count($staff_ids) === 0) {
        http_response_code(400);
        echo json_encode(["error" => "Missing or invalid 'ids' or 'token'"]);
        exit;
    }

    $staffStr = implode(',', array_map('intval', $staff_ids));

    try {
        $stmt = $pdo->prepare("CALL get_overall_stats_by_staff_ids(:staff_ids)");
        $stmt->bindParam(':staff_ids', $staffStr);
        $stmt->execute();
        $result = $stmt->fetch(PDO::FETCH_ASSOC);
        while ($stmt->nextRowset()) {;}

        echo json_encode($result, JSON_UNESCAPED_UNICODE);
    } catch (PDOException $e) {
        http_response_code(500);
        echo json_encode(["error" => "Database error: " . $e->getMessage()]);
    }
    exit;
} elseif ($segments[0] === 'staff') {
    $staffId = null;
    if (isset($segments[1]) && isset($segments[2])) {

        if (isset($_GET['id']) && is_numeric($_GET['id'])) {
            $staffId = (int)$_GET['id'];
        } elseif (isset($_GET['token'])) {
            $decoded = decode_10bit_gzip_base64url($_GET['token']);
            if (is_array($decoded) && count($decoded) === 1 && is_numeric($decoded[0])) {
                $staffId = (int)$decoded[0];
            }
        }

        if (!$staffId) {
            http_response_code(400);
            echo json_encode(['error' => 'Missing or invalid staff id']);
            exit;
        }

        try {
            if ($segments[1] === 'publications') {
                $stmt = $pdo->prepare("CALL get_publications_per_year_by_staff(?)");
            } else {
                $stmt = $pdo->prepare("CALL get_citations_per_year_by_staff(?)");
            }

            $stmt->execute([$staffId]);
            $data = $stmt->fetchAll();
            while ($stmt->nextRowset()) {;}

            echo json_encode([
                'staff_id' => $staffId,
                $segments[1] . '_per_year' => $data
            ], JSON_UNESCAPED_UNICODE);

        } catch (Exception $e) {
            http_response_code(500);
            echo json_encode(['error' => 'Server error: ' . $e->getMessage()]);
        }
        exit;
    } elseif (!isset($segments[2]) && $segments[1] === 'publications') {
        if (isset($_GET['id']) && is_numeric($_GET['id'])) {
            $staffId = (int)$_GET['id'];
        } elseif (isset($_GET['token'])) {
            $decoded = decode_10bit_gzip_base64url($_GET['token']);
            if (is_array($decoded) && count($decoded) === 1 && is_numeric($decoded[0])) {
                $staffId = (int)$decoded[0];
            }
        }

        if (!$staffId) {
            http_response_code(400);
            echo json_encode(['error' => 'Missing or invalid staff id']);
            exit;
        }

        try {
            $stmt = $pdo->prepare("CALL get_all_publications_by_staff(?)");
            $stmt->execute([$staffId]);
            $staff = $stmt->fetchAll();
            while ($stmt->nextRowset()) {;}

            echo json_encode($staff, JSON_UNESCAPED_UNICODE);
        } catch (Exception $e) {
            http_response_code(500);
            echo json_encode(['error' => 'Server error: ' . $e->getMessage()]);
        }
        exit;
    } elseif ($segments[1] === 'stats') {
        $departments = null;
        $roles = null;

        if (isset($segments[2]) && $segments[2] === 'token') {
            if (isset($_GET['departments']) && isset($_GET['roles'])) {
                $departmentsDecoded = decode_10bit_gzip_base64url($_GET['departments']);
                $rolesDecoded = decode_10bit_gzip_base64url($_GET['roles']);

                if (is_array($departmentsDecoded) && is_array($rolesDecoded)) {
                    $departments = array_map('intval', $departmentsDecoded);
                    $roles = array_map('intval', $rolesDecoded);
                }
            }
        } else {
            if (isset($_GET['departments']) && isset($_GET['roles'])) {
                $departments = array_map('intval', explode(',', $_GET['departments']));
                $roles = array_map('intval', explode(',', $_GET['roles']));
            }
        }

        if (!$departments || !$roles || !is_array($departments) || !is_array($roles)) {
            http_response_code(400);
            echo json_encode(['error' => 'Missing or invalid "departments" or "roles"']);
            exit;
        }

        $departmentsStr = implode(',', $departments);
        $rolesStr = implode(',', $roles);

        try {
            $stmt = $pdo->prepare("CALL get_all_staff_summary(?, ?)");
            $stmt->execute([$departmentsStr, $rolesStr]);
            $staff = $stmt->fetchAll();
            while ($stmt->nextRowset()) {;}

            echo json_encode($staff, JSON_UNESCAPED_UNICODE);
        } catch (Exception $e) {
            http_response_code(500);
            echo json_encode(['error' => 'Server error: ' . $e->getMessage()]);
        }
        exit;
    }

} else {
    http_response_code(405);
    echo json_encode(['error' => 'Method not allowed']);
}






